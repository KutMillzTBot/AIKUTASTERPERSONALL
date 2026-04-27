//+------------------------------------------------------------------+
//|  SupervisorEA.mq5  — FINAL EDITION v3.10                      |
//|  Confirmed trade execution + DataFeeder integration            |
//+------------------------------------------------------------------+
#property copyright "SupervisorTrainer FINAL"
#property version   "3.12"
#property strict

#include <Trade\Trade.mqh>
CTrade trade;

//── Inputs ───────────────────────────────────────────────────
input string   ServerURL       = "http://127.0.0.1:5050";
input string   TradingSymbol   = "";
input double   LotSize         = 0.01;         // ← start small
input int      SignalInterval  = 60;
input double   BuyThreshold    = 0.60;
input double   SellThreshold   = 0.40;
input bool     UseSMC_SLTP     = true;
input double   FallbackSL_Pips = 30;
input double   FallbackTP_Pips = 90;
input bool     SendFeedback    = true;
input bool     SendAccountSnapshots = true;
input bool     PrintSignals    = true;
input ulong    MagicNumber     = 20260415;
input bool     TradeEnabled    = true;         // master on/off switch
input bool     AutoMonitorTrades = true;       // monitor open trades continuously
input bool     AutoBreakEven    = true;        // move SL to break-even when RR target is hit
input double   BreakEvenRR      = 0.50;        // trigger BE when position reaches this RR
input double   BreakEvenLockPips= 1.0;         // lock this many pips above/below entry
input double   TrailingStartsAtRR = 0.50;      // Trailing Starts At RR
input double   TrailingStopMultiRR= 0.20;      // Trailing Stop Multi RR
input bool     WarmupAllWatchlist = false;     // false = fast startup heartbeat
input int      WarmupMaxSymbols   = 40;        // cap warmup when full watchlist is enabled
input int      WarmupRetries      = 1;         // retries per symbol during warmup

//── Globals ──────────────────────────────────────────────────
string   g_symbol;
datetime g_lastCheck  = 0;
double   g_lastPred   = 0.5;
int      g_lastAction = 0;
double   g_lastSL     = 0;
double   g_lastTP     = 0;
datetime g_lastUpdatePush = 0;
datetime g_lastCommandPoll = 0;
ulong    g_riskTickets[];
double   g_riskValues[];

void PushStateUpdate();
void PollBridgeCommands();
void AckBridgeCommand(string cmdId,string statusText);
void CloseSymbolPositions(string symbolName);
void CloseTicketPosition(ulong ticket);
bool ExecuteBridgeOrder(string side,string orderType,double lot,double entry,double limitPrice,double sl,double tp);
double NormalizePriceValue(string symbolName,double price);
void ManageOpenPositions();
bool PlaceStopLimitOrder(string symbolName,bool isBuy,double lot,double stopPrice,double limitPrice,double sl,double tp,string comment);
double GetOrRememberInitialRisk(ulong ticket,double entry,double sl);
void PruneTrackedRiskTickets();
void ForgetTrackedRisk(ulong ticket);

//+------------------------------------------------------------------+
int OnInit()
{
   g_symbol = (TradingSymbol == "") ? Symbol() : TradingSymbol;
   trade.SetExpertMagicNumber(MagicNumber);
   trade.SetDeviationInPoints(20);
   trade.SetTypeFilling(ORDER_FILLING_FOK);

   Print("╔══════════════════════════════════════════╗");
   Print("║  SupervisorEA  v3.11  — FINAL EDITION   ║");
   Print("╚══════════════════════════════════════════╝");
   Print("[EA] Symbol  : ", g_symbol);
   Print("[EA] Server  : ", ServerURL);
   Print("[EA] Lot     : ", LotSize);
   Print("[EA] Trading : ", TradeEnabled ? "ENABLED ✅" : "DISABLED ⛔");

   EventSetTimer(SignalInterval);
   if(SendAccountSnapshots) PushStateUpdate();

   int warmupRetries = MathMax(0, WarmupRetries);
   if(!WarmupAllWatchlist)
   {
      string watchSym = g_symbol;
      int retries = 0;
      bool gotSignal = false;
      while(retries <= warmupRetries && !gotSignal)
      {
         string url = ServerURL+"/signal?symbol="+watchSym;
         uchar pd[], resp[];
         string rqH = "Content-Type: application/json\r\n";
         string rsH = "";
         int rc = WebRequest("GET", url, rqH, 5000, pd, resp, rsH);
         if(rc == -1)
         {
            Print("[EA] ⚠️  Bridge offline for symbol ", watchSym, " (error=", GetLastError(), ")");
            retries++;
            Sleep(300);
            continue;
         }
         string json = CharArrayToString(resp);
         double score = ParseDouble(json,"score");
         string signal = ParseString(json,"signal");
         if(signal == "")
         {
            Print("[EA] ⚠️  No signal for symbol ", watchSym, " (retry ", retries+1, ")");
            retries++;
            Sleep(300);
         }
         else
         {
            Print("[EA] [INIT] ", watchSym, " signal=", signal, " score=", DoubleToString(score,4));
            gotSignal = true;
         }
      }
      if(!gotSignal)
         Print("[EA] Warmup skipped for ", watchSym, " (startup continues)");
      Print("[EA] Warmup mode: chart symbol only (fast heartbeat startup)");
      return INIT_SUCCEEDED;
   }

   int watchTotal = SymbolsTotal(true);
   int warmupLimit = watchTotal;
   if(WarmupMaxSymbols > 0 && warmupLimit > WarmupMaxSymbols)
      warmupLimit = WarmupMaxSymbols;

   int warmed = 0;
   for(int w=0; w<watchTotal && warmed < warmupLimit; w++)
   {
      string watchSym = SymbolName(w,true);
      if(watchSym == "") continue;
      warmed++;

      int retries = 0;
      bool gotSignal = false;
      while(retries <= warmupRetries && !gotSignal)
      {
         string url = ServerURL+"/signal?symbol="+watchSym;
         uchar pd[], resp[];
         string rqH = "Content-Type: application/json\r\n";
         string rsH = "";
         int rc = WebRequest("GET", url, rqH, 5000, pd, resp, rsH);
         if(rc == -1)
         {
            Print("[EA] ⚠️  Bridge offline for symbol ", watchSym, " (error=", GetLastError(), ")");
            retries++;
            Sleep(300);
            continue;
         }
         string json = CharArrayToString(resp);
         double score = ParseDouble(json,"score");
         string signal = ParseString(json,"signal");
         if(signal == "")
         {
            retries++;
            Sleep(300);
         }
         else
         {
            Print("[EA] [INIT] ", watchSym, " signal=", signal, " score=", DoubleToString(score,4));
            gotSignal = true;
         }
      }
   }
   Print("[EA] Warmup mode: full watchlist (", (string)warmed, " symbols)");
   return INIT_SUCCEEDED;
}
void OnDeinit(const int reason){ EventKillTimer(); }
void OnTimer()
{
   PollBridgeCommands();
   CheckAndTrade();
   ManageOpenPositions();
   if(SendAccountSnapshots) PushStateUpdate();
}
void OnTick()
{
   if(TimeCurrent()-g_lastCommandPoll >= 2) PollBridgeCommands();
   if(TimeCurrent()-g_lastCheck >= SignalInterval) CheckAndTrade();
   ManageOpenPositions();
   if(SendAccountSnapshots && TimeCurrent()-g_lastUpdatePush >= 5) PushStateUpdate();
}

void RefreshSymbolFromChart()
{
   if(TradingSymbol != "") return; // manual symbol override is enabled
   string chartSym = Symbol();
   if(chartSym == g_symbol) return;
   g_symbol = chartSym;
   Print("[EA] Symbol auto-switched to ", g_symbol);
}

//+------------------------------------------------------------------+
void CheckAndTrade()
{
   RefreshSymbolFromChart();
   g_lastCheck = TimeCurrent();

   //── 1. Fetch signal ──────────────────────────────────────
   string url  = ServerURL+"/signal?symbol="+g_symbol;
   uchar  pd[], resp[];
   string rqH  = "Content-Type: application/json\r\n";
   string rsH  = "";

   int rc = WebRequest("GET",url,rqH,5000,pd,resp,rsH);
   if(rc == -1)
   {
      Print("[EA] ⚠️  Bridge offline (error=",GetLastError(),")");
      Print("[EA]     Fix: python mq5_bridge_server.py");
      return;
   }

   string json   = CharArrayToString(resp);
   double score  = ParseDouble(json,"score");
   int    action = (int)ParseDouble(json,"action");
   string signal = ParseString(json,"signal");
   double sl_lvl = ParseDouble(json,"sl");
   double tp_lvl = ParseDouble(json,"tp");
   double ob_top = ParseDouble(json,"ob_top");
   double ob_bot = ParseDouble(json,"ob_bottom");
   double ob_50  = ParseDouble(json,"ob_50");

   g_lastPred   = score;
   g_lastAction = action;

   if(PrintSignals)
   {
      Print("─────────────────────────────────────────");
      Print("[EA] Signal : ",signal," | Score: ",DoubleToString(score,4));
      if(ob_50>0) Print("[EA] OB Zone: ",DoubleToString(ob_bot,5)," – ",DoubleToString(ob_top,5)," | 50%: ",DoubleToString(ob_50,5));
      if(sl_lvl>0) Print("[EA] SL=",DoubleToString(sl_lvl,5)," TP=",DoubleToString(tp_lvl,5));
   }

   if(!TradeEnabled)
   {
      Print("[EA] ⛔ Trading disabled — signal only mode");
      return;
   }

   //── 2. Execute ───────────────────────────────────────────
   double pip = SymbolInfoDouble(g_symbol,SYMBOL_POINT)*10;
   bool hasBuy  = HasPosition(POSITION_TYPE_BUY);
   bool hasSell = HasPosition(POSITION_TYPE_SELL);

   if(action>=1 && score>=BuyThreshold)
   {
      if(hasSell){ CloseAll(); Sleep(500); }
      if(!HasPosition(POSITION_TYPE_BUY))
      {
         double ask = SymbolInfoDouble(g_symbol,SYMBOL_ASK);
         double sl  = (UseSMC_SLTP && sl_lvl>0) ? sl_lvl : ask - FallbackSL_Pips*pip;
         double tp  = (UseSMC_SLTP && tp_lvl>0) ? tp_lvl : ask + FallbackTP_Pips*pip;

         // Normalise SL/TP to broker's digit requirement
         sl = NormalizeDouble(sl, (int)SymbolInfoInteger(g_symbol,SYMBOL_DIGITS));
         tp = NormalizeDouble(tp, (int)SymbolInfoInteger(g_symbol,SYMBOL_DIGITS));

         if(trade.Buy(LotSize,g_symbol,ask,sl,tp,"SupervisorEA_BUY"))
         {
            g_lastSL = sl; g_lastTP = tp;
            Print("[EA] ✅ BUY executed @ ",ask," | SL=",sl," TP=",tp,
                  " | Ticket=",trade.ResultOrder());
         }
         else
            Print("[EA] ❌ BUY FAILED retcode=",trade.ResultRetcode(),
                  " desc=",trade.ResultRetcodeDescription());
      }
   }
   else if(action<=-1 && score<=SellThreshold)
   {
      if(hasBuy){ CloseAll(); Sleep(500); }
      if(!HasPosition(POSITION_TYPE_SELL))
      {
         double bid = SymbolInfoDouble(g_symbol,SYMBOL_BID);
         double sl  = (UseSMC_SLTP && sl_lvl>0) ? sl_lvl : bid + FallbackSL_Pips*pip;
         double tp  = (UseSMC_SLTP && tp_lvl>0) ? tp_lvl : bid - FallbackTP_Pips*pip;

         sl = NormalizeDouble(sl,(int)SymbolInfoInteger(g_symbol,SYMBOL_DIGITS));
         tp = NormalizeDouble(tp,(int)SymbolInfoInteger(g_symbol,SYMBOL_DIGITS));

         if(trade.Sell(LotSize,g_symbol,bid,sl,tp,"SupervisorEA_SELL"))
         {
            g_lastSL = sl; g_lastTP = tp;
            Print("[EA] ✅ SELL executed @ ",bid," | SL=",sl," TP=",tp,
                  " | Ticket=",trade.ResultOrder());
         }
         else
            Print("[EA] ❌ SELL FAILED retcode=",trade.ResultRetcode(),
                  " desc=",trade.ResultRetcodeDescription());
      }
   }
   else
      Print("[EA] ⏸  HOLD (",DoubleToString(score,4),")");
}

//── Feedback on close ────────────────────────────────────────
void OnTradeTransaction(const MqlTradeTransaction& trans,
                        const MqlTradeRequest& req,
                        const MqlTradeResult&  res_t)
{
   if(!SendFeedback) return;
   if(trans.type != TRADE_TRANSACTION_DEAL_ADD) return;

   HistoryDealSelect(trans.deal);
   long dealEntry = HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
   if(dealEntry == DEAL_ENTRY_OUT || dealEntry == DEAL_ENTRY_OUT_BY)
   {
      ulong closedPosId = (ulong)HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID);
      if(closedPosId > 0) ForgetTrackedRisk(closedPosId);
   }
   double profit = HistoryDealGetDouble(trans.deal,DEAL_PROFIT);
   if(profit==0) return;

   int won = (profit>0)?1:0;
   string body =
      "{\"symbol\":\""     + g_symbol    + "\","
      "\"model_name\":\"ensemble\","
      "\"prediction\":"    + DoubleToString(g_lastPred,4) + ","
      "\"actual\":"        + IntegerToString(won) + ","
      "\"profit\":"        + DoubleToString(profit,2) + "}";

   uchar  pbuf[],fbuf[];
   string fbH="Content-Type: application/json\r\n", rfH="";
   StringToCharArray(body,pbuf,0,StringLen(body));
   WebRequest("POST",ServerURL+"/trade_result",fbH,5000,pbuf,fbuf,rfH);
   Print("[EA] 📬 Feedback → won=",won," profit=",DoubleToString(profit,2));
}

void PushStateUpdate()
{
   g_lastUpdatePush = TimeCurrent();
   string body = "{";
   body += "\"symbol\":\"" + g_symbol + "\",";
   body += "\"watchlist\":[";
   int watchTotal = SymbolsTotal(true);
   bool wroteWatch = false;
   for(int w=0; w<watchTotal; w++)
   {
      string watchSym = SymbolName(w,true);
      if(watchSym == "") continue;
      if(wroteWatch) body += ",";
      body += "\"" + watchSym + "\"";
      wroteWatch = true;
   }
   // If Market Watch looks empty, fall back to all visible symbols.
   if(!wroteWatch)
   {
      int allTotal = SymbolsTotal(false);
      for(int a=0; a<allTotal; a++)
      {
         string visSym = SymbolName(a,false);
         if(visSym == "") continue;
         long isVisible = 0;
         if(!SymbolInfoInteger(visSym, SYMBOL_VISIBLE, isVisible)) continue;
         if(isVisible == 0) continue;
         if(wroteWatch) body += ",";
         body += "\"" + visSym + "\"";
         wroteWatch = true;
      }
   }
   string activeChartSymbol = Symbol();
   if(activeChartSymbol != "")
   {
      if(wroteWatch) body += ",";
      body += "\"" + activeChartSymbol + "\"";
      wroteWatch = true;
   }
   if(g_symbol != "" && g_symbol != activeChartSymbol)
   {
      if(wroteWatch) body += ",";
      body += "\"" + g_symbol + "\"";
   }
   body += "],";
   body += "\"account\":{";
   body += "\"balance\":"      + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE),2) + ",";
   body += "\"equity\":"       + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY),2) + ",";
   body += "\"margin\":"       + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN),2) + ",";
   body += "\"free_margin\":"  + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_FREE),2) + ",";
   body += "\"margin_level\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN_LEVEL),2) + ",";
   body += "\"broker\":\""     + AccountInfoString(ACCOUNT_COMPANY) + "\",";
   body += "\"currency\":\""   + AccountInfoString(ACCOUNT_CURRENCY) + "\",";
   body += "\"leverage\":"     + IntegerToString((int)AccountInfoInteger(ACCOUNT_LEVERAGE));
   body += "},";
   body += "\"positions\":[";

   bool first = true;
   int totalPos = PositionsTotal();
   for(int i=0;i<totalPos;i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket==0) continue;
      if(!PositionSelectByTicket(ticket)) continue;

      string psym = PositionGetString(POSITION_SYMBOL);
      string ptype = ((ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY) ? "BUY" : "SELL";
      double lot = PositionGetDouble(POSITION_VOLUME);
      double entry = PositionGetDouble(POSITION_PRICE_OPEN);
      double sl = PositionGetDouble(POSITION_SL);
      double tp = PositionGetDouble(POSITION_TP);
      double pnl = PositionGetDouble(POSITION_PROFIT);

      if(!first) body += ",";
      first = false;
      body += "{";
      body += "\"ticket\":\"" + (string)ticket + "\",";
      body += "\"symbol\":\"" + psym + "\",";
      body += "\"type\":\""   + ptype + "\",";
      body += "\"lot\":"      + DoubleToString(lot,2) + ",";
      body += "\"entry\":"    + DoubleToString(entry,5) + ",";
      body += "\"sl\":"       + DoubleToString(sl,5) + ",";
      body += "\"tp\":"       + DoubleToString(tp,5) + ",";
      body += "\"pnl\":"      + DoubleToString(pnl,2);
      body += "}";
   }
   body += "]";
   body += ",\"quotes\":[";

   bool firstQuote = true;
   int quoteCount = 0;
   int quoteLimit = 60;
   for(int q=0; q<watchTotal && quoteCount<quoteLimit; q++)
   {
      string qsym = SymbolName(q,true);
      if(qsym == "") continue;

      double bid = SymbolInfoDouble(qsym, SYMBOL_BID);
      double ask = SymbolInfoDouble(qsym, SYMBOL_ASK);
      if(bid <= 0.0 || ask <= 0.0) continue;

      double mid = (bid + ask) / 2.0;
      double dayOpen = iOpen(qsym, PERIOD_D1, 0);
      double dailyChange = (dayOpen > 0.0) ? (mid - dayOpen) : 0.0;
      double dailyChangePct = (dayOpen > 0.0) ? ((dailyChange / dayOpen) * 100.0) : 0.0;

      if(!firstQuote) body += ",";
      firstQuote = false;
      quoteCount++;

      body += "{";
      body += "\"symbol\":\"" + qsym + "\",";
      body += "\"bid\":" + DoubleToString(bid,8) + ",";
      body += "\"ask\":" + DoubleToString(ask,8) + ",";
      body += "\"mid\":" + DoubleToString(mid,8) + ",";
      body += "\"day_open\":" + DoubleToString(dayOpen,8) + ",";
      body += "\"daily_change\":" + DoubleToString(dailyChange,8) + ",";
      body += "\"daily_change_pct\":" + DoubleToString(dailyChangePct,4) + ",";
      body += "\"timestamp\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\"";
      body += "}";
   }

    if(quoteCount == 0 && g_symbol != "")
   {
      double bid = SymbolInfoDouble(g_symbol, SYMBOL_BID);
      double ask = SymbolInfoDouble(g_symbol, SYMBOL_ASK);
      if(bid > 0.0 && ask > 0.0)
      {
         double mid = (bid + ask) / 2.0;
         double dayOpen = iOpen(g_symbol, PERIOD_D1, 0);
         double dailyChange = (dayOpen > 0.0) ? (mid - dayOpen) : 0.0;
         double dailyChangePct = (dayOpen > 0.0) ? ((dailyChange / dayOpen) * 100.0) : 0.0;

         if(!firstQuote) body += ",";
         body += "{";
         body += "\"symbol\":\"" + g_symbol + "\",";
         body += "\"bid\":" + DoubleToString(bid,8) + ",";
         body += "\"ask\":" + DoubleToString(ask,8) + ",";
         body += "\"mid\":" + DoubleToString(mid,8) + ",";
         body += "\"day_open\":" + DoubleToString(dayOpen,8) + ",";
         body += "\"daily_change\":" + DoubleToString(dailyChange,8) + ",";
         body += "\"daily_change_pct\":" + DoubleToString(dailyChangePct,4) + ",";
         body += "\"timestamp\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\"";
         body += "}";
      }
   }

   body += "]";
   body += "}";

   uchar pbuf[], rbuf[];
   string hdr = "Content-Type: application/json\r\n", rh = "";
   StringToCharArray(body,pbuf,0,StringLen(body));
   WebRequest("POST",ServerURL+"/update",hdr,5000,pbuf,rbuf,rh);
}

void PollBridgeCommands()
{
   g_lastCommandPoll = TimeCurrent();
   string url = ServerURL + "/ea/command?symbol=" + g_symbol;
   uchar pd[], resp[];
   string hdr = "Content-Type: application/json\r\n", rh = "";
   int rc = WebRequest("GET", url, hdr, 4000, pd, resp, rh);
   if(rc == -1) return;

   string json = CharArrayToString(resp);
   string cmdId = ParseString(json, "id");
   string cmd = ParseString(json, "cmd");
   string cmdSymbol = ParseString(json, "symbol");
   string cmdTicket = ParseString(json, "ticket");
   string side = ParseString(json, "side");
   string orderType = ParseString(json, "order_type");
   double lot = ParseDouble(json, "lot");
   double entry = ParseDouble(json, "entry");
   double limitPrice = ParseDouble(json, "limit_price");
   double sl = ParseDouble(json, "sl");
   double tp = ParseDouble(json, "tp");
   if(cmd == "" || cmd == "none") return;

   bool executed = false;
   if(cmd == "close_all")
   {
      CloseAll();
      executed = true;
   }
   else if(cmd == "close_symbol")
   {
      if(cmdSymbol == "" || cmdSymbol == g_symbol)
      {
         CloseSymbolPositions(g_symbol);
         executed = true;
      }
   }
   else if(cmd == "close_ticket")
   {
      ulong ticket = (ulong)StringToInteger(cmdTicket);
      if(ticket > 0)
      {
         CloseTicketPosition(ticket);
         executed = true;
      }
   }
   else if(cmd == "place_order")
   {
      if(cmdSymbol == "" || cmdSymbol == g_symbol)
         executed = ExecuteBridgeOrder(side, orderType, lot, entry, limitPrice, sl, tp);
   }

   if(cmdId != "")
      AckBridgeCommand(cmdId, executed ? "done" : "ignored");

   if(executed && SendAccountSnapshots)
      PushStateUpdate();
}

void AckBridgeCommand(string cmdId,string statusText)
{
   string body = "{\"id\":\"" + cmdId + "\",\"status\":\"" + statusText + "\",\"symbol\":\"" + g_symbol + "\"}";
   uchar pbuf[], rbuf[];
   string hdr = "Content-Type: application/json\r\n", rh = "";
   StringToCharArray(body,pbuf,0,StringLen(body));
   WebRequest("POST",ServerURL+"/ea/command_ack",hdr,5000,pbuf,rbuf,rh);
}

void CloseSymbolPositions(string symbolName)
{
   for(int i=PositionsTotal()-1;i>=0;i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket==0) continue;
      if(!PositionSelectByTicket(ticket)) continue;
      if(PositionGetString(POSITION_SYMBOL) != symbolName) continue;
      if((ulong)PositionGetInteger(POSITION_MAGIC) != MagicNumber) continue;
      trade.PositionClose(ticket);
   }
}

void CloseTicketPosition(ulong ticket)
{
   if(ticket == 0) return;
   if(!PositionSelectByTicket(ticket)) return;
   if((ulong)PositionGetInteger(POSITION_MAGIC) != MagicNumber) return;
   trade.PositionClose(ticket);
}

//── Helpers ──────────────────────────────────────────────────
bool HasPosition(ENUM_POSITION_TYPE pt)
{
   for(int i=0;i<PositionsTotal();i++)
      if(PositionGetSymbol(i)==g_symbol &&
         (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE)==pt &&
         (ulong)PositionGetInteger(POSITION_MAGIC)==MagicNumber) return true;
   return false;
}
void CloseAll()
{
   for(int i=PositionsTotal()-1;i>=0;i--)
      if(PositionGetSymbol(i)==g_symbol &&
         (ulong)PositionGetInteger(POSITION_MAGIC)==MagicNumber)
         trade.PositionClose(PositionGetTicket(i));
}

int FindTrackedRiskIndex(ulong ticket)
{
   int total = ArraySize(g_riskTickets);
   for(int i=0; i<total; i++)
      if(g_riskTickets[i] == ticket)
         return i;
   return -1;
}

void ForgetTrackedRisk(ulong ticket)
{
   int idx = FindTrackedRiskIndex(ticket);
   if(idx < 0) return;

   int total = ArraySize(g_riskTickets);
   for(int i=idx; i<total-1; i++)
   {
      g_riskTickets[i] = g_riskTickets[i+1];
      g_riskValues[i] = g_riskValues[i+1];
   }
   if(total > 0)
   {
      ArrayResize(g_riskTickets, total - 1);
      ArrayResize(g_riskValues, total - 1);
   }
}

double GetOrRememberInitialRisk(ulong ticket,double entry,double sl)
{
   double risk = MathAbs(entry - sl);
   if(risk <= 0.0) return 0.0;

   int idx = FindTrackedRiskIndex(ticket);
   if(idx >= 0)
   {
      if(g_riskValues[idx] <= 0.0)
         g_riskValues[idx] = risk;
      return g_riskValues[idx];
   }

   int total = ArraySize(g_riskTickets);
   ArrayResize(g_riskTickets, total + 1);
   ArrayResize(g_riskValues, total + 1);
   g_riskTickets[total] = ticket;
   g_riskValues[total] = risk;
   return risk;
}

void PruneTrackedRiskTickets()
{
   for(int i=ArraySize(g_riskTickets)-1; i>=0; i--)
   {
      ulong ticket = g_riskTickets[i];
      if(ticket == 0)
      {
         ForgetTrackedRisk(ticket);
         continue;
      }
      if(!PositionSelectByTicket(ticket))
      {
         ForgetTrackedRisk(ticket);
         continue;
      }
      if((ulong)PositionGetInteger(POSITION_MAGIC) != MagicNumber)
      {
         ForgetTrackedRisk(ticket);
         continue;
      }
   }
}

void ManageOpenPositions()
{
   if(!AutoMonitorTrades) return;
   if(!AutoBreakEven && (TrailingStopMultiRR <= 0.0 || TrailingStartsAtRR <= 0.0)) return;

   PruneTrackedRiskTickets();

   for(int i=PositionsTotal()-1;i>=0;i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(!PositionSelectByTicket(ticket)) continue;
      if((ulong)PositionGetInteger(POSITION_MAGIC) != MagicNumber) continue;

      string symbolName = PositionGetString(POSITION_SYMBOL);
      ENUM_POSITION_TYPE ptype = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      double entry = PositionGetDouble(POSITION_PRICE_OPEN);
      double sl = PositionGetDouble(POSITION_SL);
      double tp = PositionGetDouble(POSITION_TP);
      if(entry <= 0.0 || sl <= 0.0) continue;

      double initialRisk = GetOrRememberInitialRisk(ticket, entry, sl);
      if(initialRisk <= 0.0) continue;

      double marketPrice = (ptype == POSITION_TYPE_BUY)
         ? SymbolInfoDouble(symbolName,SYMBOL_BID)
         : SymbolInfoDouble(symbolName,SYMBOL_ASK);
      if(marketPrice <= 0.0) continue;

      double reward = (ptype == POSITION_TYPE_BUY)
         ? (marketPrice - entry)
         : (entry - marketPrice);
      if(reward <= 0.0) continue;

      double rr = reward / initialRisk;
      if(rr <= 0.0) continue;

      double point = SymbolInfoDouble(symbolName,SYMBOL_POINT);
      int digits = (int)SymbolInfoInteger(symbolName,SYMBOL_DIGITS);
      long stopsLevelPoints = (long)SymbolInfoInteger(symbolName,SYMBOL_TRADE_STOPS_LEVEL);
      double minGap = MathMax(point, (double)stopsLevelPoints * point);
      double targetSL = sl;
      bool hasTarget = false;

      if(AutoBreakEven && rr >= BreakEvenRR)
      {
         double pip = point * 10.0;
         double beLock = BreakEvenLockPips * pip;
         double beSL = (ptype == POSITION_TYPE_BUY) ? (entry + beLock) : (entry - beLock);
         beSL = NormalizeDouble(beSL, digits);

         if(ptype == POSITION_TYPE_BUY)
         {
            if(beSL > targetSL)
            {
               targetSL = beSL;
               hasTarget = true;
            }
         }
         else
         {
            if(beSL < targetSL)
            {
               targetSL = beSL;
               hasTarget = true;
            }
         }
      }

      if(TrailingStopMultiRR > 0.0 && rr >= TrailingStartsAtRR)
      {
         double trailRR = rr - TrailingStopMultiRR;
         if(trailRR > 0.0)
         {
            double trailSL = (ptype == POSITION_TYPE_BUY)
               ? (entry + initialRisk * trailRR)
               : (entry - initialRisk * trailRR);

            if(ptype == POSITION_TYPE_BUY)
            {
               double maxAllowed = marketPrice - minGap;
               if(maxAllowed > 0.0 && trailSL > maxAllowed) trailSL = maxAllowed;
               if(tp > 0.0 && trailSL >= tp - point) trailSL = tp - point;
               if(trailSL > targetSL)
               {
                  targetSL = trailSL;
                  hasTarget = true;
               }
            }
            else
            {
               double minAllowed = marketPrice + minGap;
               if(minAllowed > 0.0 && trailSL < minAllowed) trailSL = minAllowed;
               if(tp > 0.0 && trailSL <= tp + point) trailSL = tp + point;
               if(trailSL < targetSL)
               {
                  targetSL = trailSL;
                  hasTarget = true;
               }
            }
         }
      }

      if(!hasTarget) continue;

      if(ptype == POSITION_TYPE_BUY)
      {
         double maxAllowed = marketPrice - minGap;
         if(maxAllowed > 0.0 && targetSL > maxAllowed) targetSL = maxAllowed;
         if(tp > 0.0 && targetSL >= tp - point) targetSL = tp - point;
         if(targetSL <= sl + point) continue;
      }
      else
      {
         double minAllowed = marketPrice + minGap;
         if(minAllowed > 0.0 && targetSL < minAllowed) targetSL = minAllowed;
         if(tp > 0.0 && targetSL <= tp + point) targetSL = tp + point;
         if(targetSL >= sl - point) continue;
      }

      targetSL = NormalizeDouble(targetSL, digits);
      if(targetSL <= 0.0) continue;

      if(trade.PositionModify(ticket, targetSL, tp))
      {
         Print("[EA] SL updated ticket=", (string)ticket,
               " symbol=", symbolName,
               " RR=", DoubleToString(rr,2),
               " newSL=", DoubleToString(targetSL,digits));
      }
      else
      {
         Print("[EA] SL modify failed ticket=", (string)ticket,
               " retcode=", trade.ResultRetcode(),
               " desc=", trade.ResultRetcodeDescription());
      }
   }
}
double ParseDouble(string j,string k)
{
   string s="\""+k+"\":";
   int p=StringFind(j,s); if(p<0) return 0.0;
   p+=StringLen(s);
   while(p<StringLen(j)&&(StringGetCharacter(j,p)==' '||StringGetCharacter(j,p)=='"'))p++;
   string v="";
   while(p<StringLen(j)){ushort c=StringGetCharacter(j,p);if(c==','||c=='}'||c=='"')break;v+=ShortToString(c);p++;}
   return StringToDouble(v);
}
string ParseString(string j,string k)
{
   string s="\""+k+"\":\"";
   int p=StringFind(j,s); if(p<0) return "";
   p+=StringLen(s); string v="";
   while(p<StringLen(j)&&StringGetCharacter(j,p)!='"'){v+=ShortToString(StringGetCharacter(j,p));p++;}
   return v;
}

double NormalizePriceValue(string symbolName,double price)
{
   int digits = (int)SymbolInfoInteger(symbolName,SYMBOL_DIGITS);
   return NormalizeDouble(price, digits);
}

bool PlaceStopLimitOrder(string symbolName,bool isBuy,double lot,double stopPrice,double limitPrice,double sl,double tp,string comment)
{
   MqlTradeRequest req;
   MqlTradeResult res;
   ZeroMemory(req);
   ZeroMemory(res);

   req.action = TRADE_ACTION_PENDING;
   req.symbol = symbolName;
   req.volume = lot;
   req.magic = MagicNumber;
   req.deviation = 20;
   req.type_time = ORDER_TIME_GTC;
   req.type_filling = ORDER_FILLING_RETURN;
   req.type = isBuy ? ORDER_TYPE_BUY_STOP_LIMIT : ORDER_TYPE_SELL_STOP_LIMIT;
   req.price = stopPrice;
   req.stoplimit = limitPrice;
   req.sl = sl;
   req.tp = tp;
   req.comment = comment;

   bool sent = OrderSend(req,res);
   if(!sent)
   {
      Print("[EA] Stop-limit send failed err=", GetLastError(), " symbol=", symbolName);
      return false;
   }

   bool placed = (res.retcode == TRADE_RETCODE_DONE || res.retcode == TRADE_RETCODE_PLACED);
   if(!placed)
      Print("[EA] Stop-limit rejected retcode=", (string)res.retcode, " comment=", res.comment);
   return placed;
}

bool ExecuteBridgeOrder(string side,string orderType,double lot,double entry,double limitPrice,double sl,double tp)
{
   string type = orderType;
   string sideNorm = side;
   StringToUpper(type);
   StringToUpper(sideNorm);
   if(sideNorm == "")
      sideNorm = (StringFind(type,"SELL") == 0) ? "SELL" : "BUY";
   if(type == "")
      type = sideNorm + "_MARKET";

   if(lot <= 0.0) lot = LotSize;
   if(entry <= 0.0)
      entry = (sideNorm == "SELL") ? SymbolInfoDouble(g_symbol,SYMBOL_BID) : SymbolInfoDouble(g_symbol,SYMBOL_ASK);

   entry = NormalizePriceValue(g_symbol, entry);
   if(limitPrice > 0.0) limitPrice = NormalizePriceValue(g_symbol, limitPrice);
   if(sl > 0.0) sl = NormalizePriceValue(g_symbol, sl);
   if(tp > 0.0) tp = NormalizePriceValue(g_symbol, tp);

   bool ok = false;
   if(type == "BUY" || type == "BUY_MARKET")
      ok = trade.Buy(lot,g_symbol,entry,sl,tp,"SupervisorEA_MANUAL_BUY");
   else if(type == "SELL" || type == "SELL_MARKET")
      ok = trade.Sell(lot,g_symbol,entry,sl,tp,"SupervisorEA_MANUAL_SELL");
   else if(type == "BUY_LIMIT")
      ok = trade.BuyLimit(lot,entry,g_symbol,sl,tp,ORDER_TIME_GTC,0,"SupervisorEA_BUY_LIMIT");
   else if(type == "SELL_LIMIT")
      ok = trade.SellLimit(lot,entry,g_symbol,sl,tp,ORDER_TIME_GTC,0,"SupervisorEA_SELL_LIMIT");
   else if(type == "BUY_STOP")
      ok = trade.BuyStop(lot,entry,g_symbol,sl,tp,ORDER_TIME_GTC,0,"SupervisorEA_BUY_STOP");
   else if(type == "SELL_STOP")
      ok = trade.SellStop(lot,entry,g_symbol,sl,tp,ORDER_TIME_GTC,0,"SupervisorEA_SELL_STOP");
   else if(type == "BUY_STOP_LIMIT")
   {
      if(limitPrice <= 0.0) limitPrice = entry;
      ok = PlaceStopLimitOrder(g_symbol,true,lot,entry,limitPrice,sl,tp,"SupervisorEA_BUY_STOP_LIMIT");
   }
   else if(type == "SELL_STOP_LIMIT")
   {
      if(limitPrice <= 0.0) limitPrice = entry;
      ok = PlaceStopLimitOrder(g_symbol,false,lot,entry,limitPrice,sl,tp,"SupervisorEA_SELL_STOP_LIMIT");
   }

   if(ok)
      Print("[EA] Manual order executed: ", type, " lot=", DoubleToString(lot,2), " entry=", DoubleToString(entry,5));
   else
      Print("[EA] Manual order failed: ", type, " retcode=", trade.ResultRetcode(), " desc=", trade.ResultRetcodeDescription());

   return ok;
}
//+------------------------------------------------------------------+
