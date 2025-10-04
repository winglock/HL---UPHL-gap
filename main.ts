import * as hl from "@nktkas/hyperliquid";
import WebSocket from "ws";
import * as fs from "fs/promises";
import * as path from "path";
import fetch from "node-fetch";
import { createWriteStream, WriteStream } from "fs";
import { stringify } from "csv-stringify";
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import Decimal from 'decimal.js';
import {
  Token,
  CurrencyAmount,
  TradeType,
  Percent,
} from "@uniswap/sdk-core";
import { Pool, Route, Trade } from "@uniswap/v3-sdk";
import JSBI from "jsbi";
import { once } from "events";

// Configure Decimal.js
Decimal.set({ precision: 60, rounding: Decimal.ROUND_HALF_UP });

// ==================== 인터페이스 정의 ====================
interface OrderbookLevel {
  level: number;
  price: string;
  size: string;
  cumulative: string;
  timestamp: string;
}

interface CoinOrderbook {
  symbol: string;
  displaySymbol: string;
  lastUpdate: string;
  midPrice: string;
  spread: string;
  spreadPercentage: string;
  bestBid: number;
  bestAsk: number;
  bids: OrderbookLevel[];
  asks: OrderbookLevel[];
}

interface DexPoolData {
  symbol: string;
  poolAddress: string;
  price: number;
  priceInUSDT: number;
  liquidity?: string;
  token0Symbol: string;
  token1Symbol: string;
  sqrtPrice?: string;
  feeTier?: number;
  tick?: number;
  token0?: { address: string; decimals: number };
  token1?: { address: string; decimals: number };
}

interface PriceImpact {
  protocol: string;
  side: "BUY" | "SELL";
  targetAmount: number;
  executionPrice: number;
  priceImpactPercent: number;
  slippage: number;
  feasible: boolean;
}

interface ArbitrageRoute {
  token: string;
  direction: string;
  buyProtocol: string;
  sellProtocol: string;
  buyPrice: number;
  sellPrice: number;
  priceGap: number;
  optimalAmount: number;
  netProfit: number;
  estimatedProfitUSD: number;
  buyPriceImpact: number;
  sellPriceImpact: number;
  buyExecutionPrice: number;
  sellExecutionPrice: number;
  status: "PROFITABLE" | "NEGATIVE" | "NO_DATA";
}

// ==================== TTL 캐시 구현 ====================
class TTLMap<K, V> {
  private data: Map<K, { v: V; t: number }>;
  private ttlMs: number;

  constructor(ttlMs = 60_000) {
    this.data = new Map();
    this.ttlMs = ttlMs;
  }

  set(k: K, v: V): this {
    this.data.set(k, { v, t: Date.now() });
    return this;
  }

  get(k: K): V | undefined {
    const ent = this.data.get(k);
    if (ent && Date.now() - ent.t < this.ttlMs) return ent.v;
    this.data.delete(k);
    return undefined;
  }

  getValid(k: K): V | undefined {
    return this.get(k);
  }

  has(k: K): boolean {
    const ent = this.data.get(k);
    if (ent && Date.now() - ent.t < this.ttlMs) return true;
    this.data.delete(k);
    return false;
  }

  delete(k: K): boolean {
    return this.data.delete(k);
  }

  sweep(): void {
    const now = Date.now();
    for (const [k, { t }] of this.data) {
      if (now - t >= this.ttlMs) this.data.delete(k);
    }
  }

  getAllValid(): Map<K, V> {
    const result = new Map<K, V>();
    const now = Date.now();
    for (const [k, { v, t }] of this.data) {
      if (now - t < this.ttlMs) {
        result.set(k, v);
      } else {
        this.data.delete(k);
      }
    }
    return result;
  }

  get size(): number {
    this.sweep();
    return this.data.size;
  }
}

// ==================== 전역 변수 및 상수 ====================
const TARGET_SYMBOLS = ["PUP", "HYPE", "UPHL"];
const SUBGRAPH_URL = "https://api.upheaval.fi/subgraphs/name/upheaval/exchange-v3";

const HYPERLIQUID_FEE_BPS = 7;    // 0.07% = 7bp taker fee
const UPHEAVAL_FEE_BPS = 9.7;     // 0.097% = 9.7bp pool fee
const HL_MAX_NOTIONAL = 10_000;

let isRunning = true;
const spotSymbolMapping = new Map<string, string>();
const reverseLookup = new Map<string, string>();

const httpTransport = new hl.HttpTransport();
const publicClient = new hl.PublicClient({ transport: httpTransport });

const incrementalOrderbooks = new TTLMap<string, CoinOrderbook>(30_000);
const targetMidsCache = new Map<string, string>();
const dexPrices = new Map<string, DexPoolData>();

let outputDir: string;
let dexUpdateInterval: NodeJS.Timeout;
let ttlSweepInterval: NodeJS.Timeout;
let csvWriteStream: WriteStream | null = null;
let csvStringifier: any = null;

// ==================== CSV 스트리밍 초기화 ====================
async function initializeCSVStream(): Promise<void> {
  if (!csvWriteStream) {
    const timestamp = new Date().toISOString().replace(/:/g, "-").split(".")[0];
    const csvPath = path.join(outputDir, `arbitrage_routes_${timestamp}.csv`);
    
    csvWriteStream = createWriteStream(csvPath);
    csvStringifier = stringify({
      header: true,
      columns: {
        timestamp: 'Timestamp',
        token: 'Token',
        direction: 'Direction',
        buyProtocol: 'Buy_Protocol',
        sellProtocol: 'Sell_Protocol',
        buyPrice: 'Buy_Price',
        sellPrice: 'Sell_Price',
        buyExecPrice: 'Buy_Exec_Price',
        sellExecPrice: 'Sell_Exec_Price',
        priceGap: 'Price_Gap_Pct',
        netProfit: 'Net_Profit_Pct',
        estimatedProfit: 'Est_Profit_USD',
        buyPriceImpact: 'Buy_Impact_Pct',
        sellPriceImpact: 'Sell_Impact_Pct',
        optimalAmount: 'Optimal_Amount_USD',
        status: 'Status'
      }
    });
    
    csvStringifier.pipe(csvWriteStream);
  }
}

async function appendToCSV(routes: ArbitrageRoute[]): Promise<void> {
  if (!csvStringifier || !isRunning) return;

  try {
    const timestamp = new Date().toISOString();
    for (const route of routes) {
      const ok = csvStringifier.write({
        timestamp,
        token: route.token,
        direction: route.direction,
        buyProtocol: route.buyProtocol,
        sellProtocol: route.sellProtocol,
        buyPrice: route.buyPrice.toFixed(8),
        sellPrice: route.sellPrice.toFixed(8),
        buyExecPrice: route.buyExecutionPrice.toFixed(8),
        sellExecPrice: route.sellExecutionPrice.toFixed(8),
        priceGap: route.priceGap.toFixed(4),
        netProfit: route.netProfit > -999 ? route.netProfit.toFixed(4) : "N/A",
        estimatedProfit: route.estimatedProfitUSD.toFixed(2),
        buyPriceImpact: route.buyPriceImpact.toFixed(4),
        sellPriceImpact: route.sellPriceImpact.toFixed(4),
        optimalAmount: route.optimalAmount.toFixed(2),
        status: route.status
      });

      if (!ok) await once(csvStringifier, "drain");
    }
  } catch (error) {
    console.error("[CSV] 저장 오류:", error);
  }
}

// ==================== 유틸리티 ====================
function calculatePriceFromSqrtPrice(
  sqrtPriceX96: string,
  decimals0: number,
  decimals1: number
): number {
  try {
    const Q96 = new Decimal(2).pow(96);
    const sqrtP = new Decimal(sqrtPriceX96);
    const price = sqrtP.div(Q96).pow(2);
    const adj = new Decimal(10).pow(decimals0 - decimals1);
    return price.mul(adj).toNumber();
  } catch (e) {
    console.error("[UTIL] sqrtPrice precise calc error:", e);
    return 0;
  }
}

// Pool 객체 생성 유틸리티 추가
function poolFromDexData(d: DexPoolData): Pool | null {
  if (!(d.token0 && d.token1 && d.sqrtPrice && d.liquidity && d.tick)) return null;

  try {
    const token0 = new Token(1, d.token0.address, d.token0.decimals, d.token0Symbol);
    const token1 = new Token(1, d.token1.address, d.token1.decimals, d.token1Symbol);

    return new Pool(
      token0,
      token1,
      d.feeTier ?? 3000,
      JSBI.BigInt(d.sqrtPrice),
      JSBI.BigInt(d.liquidity),
      d.tick
    );
  } catch (error) {
    console.error("[DEX] Pool 객체 생성 실패:", error);
    return null;
  }
}

// ==================== HTTP 호출 최적화 ====================
async function fetchAllMids(): Promise<Record<string, string>> {
  const res = await fetch("https://api.hyperliquid.xyz/info", {
    method: "POST",
    body: JSON.stringify({ type: "allMids" }),
    headers: { "Content-Type": "application/json" }
  });
  if (!res.ok) throw new Error("allMids fetch failed");
  const data = await res.json();
  return data as Record<string, string>;
}

async function updateTargetMidPricesOptimized(): Promise<void> {
  try {
    const allMids = await fetchAllMids();
    TARGET_SYMBOLS.forEach(token => {
      const displaySymbol = `${token}-USDT`;
      const internalSymbol = reverseLookup.get(displaySymbol);
      if (internalSymbol && allMids[internalSymbol]) {
        targetMidsCache.set(internalSymbol, allMids[internalSymbol]);
      }
    });
  } catch (error) {
    console.error("[HL] Mid price 업데이트 실패:", error);
  }
}

// ==================== DEX 데이터 ====================
async function getDexPoolPrice(poolId: string, poolName: string): Promise<DexPoolData | null> {
  const graphqlQuery = {
    operationName: "GetPoolInfo",
    variables: { poolId },
    query: `
      query GetPoolInfo($poolId: String!) {
        pool(id: $poolId) {
          id
          sqrtPrice
          liquidity
          feeTier
          tick
          token0 { 
            id 
            symbol 
            decimals 
          }
          token1 { 
            id 
            symbol 
            decimals 
          }
        }
      }
    `,
  };

  try {
    const response = await fetch(SUBGRAPH_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(graphqlQuery),
    });
    
    if (!response.ok) return null;
    
    const data: any = await response.json();
    if (data.errors || !data.data?.pool) return null;

    const pool = data.data.pool;
    const token0 = pool.token0;
    const token1 = pool.token1;
    const price = calculatePriceFromSqrtPrice(
      pool.sqrtPrice,
      parseInt(token0.decimals),
      parseInt(token1.decimals)
    );

    return {
      symbol: poolName,
      poolAddress: poolId,
      price,
      priceInUSDT: 0,
      liquidity: pool.liquidity,
      token0Symbol: token0.symbol,
      token1Symbol: token1.symbol,
      sqrtPrice: pool.sqrtPrice,
      feeTier: parseInt(pool.feeTier),
      tick: parseInt(pool.tick),
      token0: {
        address: token0.id,
        decimals: parseInt(token0.decimals),
      },
      token1: {
        address: token1.id,
        decimals: parseInt(token1.decimals),
      },
    };
  } catch (error) {
    console.error(`[DEX] ${poolName} 가격 조회 실패:`, error);
    return null;
  }
}

async function updateAllDexPrices(): Promise<void> {
  const targetPools = {
    "PUP/WHYPE": "0xe9c02ca07931f9670fa87217372b3c9aa5a8a934",
    "HYPE/USDT0": "0x6df19a40aaf19d01c2616a1765d4d2a4842bffaf",
    "UPHL/WHYPE": "0x49c6e25f156eea9fe340e25087c76a6aad7c610a",
  };

  const poolPromises = Object.entries(targetPools).map(([name, poolAddress]) =>
    getDexPoolPrice(poolAddress, name).then(data => ({ name, data }))
  );

  try {
    const results = await Promise.all(poolPromises);
    
    const hypeResult = results.find(r => r.name === "HYPE/USDT0");
    if (!hypeResult?.data) {
      console.error("[DEX] HYPE 기준가 조회 실패");
      return;
    }

    const whypePrice = hypeResult.data.token0Symbol === "WHYPE" 
      ? hypeResult.data.price 
      : 1 / hypeResult.data.price;
    
    dexPrices.set("HYPE", { ...hypeResult.data, priceInUSDT: whypePrice });

    results.forEach(({ name, data }) => {
      if (name === "HYPE/USDT0" || !data) return;

      const targetToken = data.token0Symbol === "WHYPE" 
        ? data.token1Symbol 
        : data.token0Symbol;
      
      const priceInWhype = data.token1Symbol === "WHYPE" 
        ? data.price 
        : 1 / data.price;
      
      const priceInUSDT = priceInWhype * whypePrice;
      dexPrices.set(targetToken, { ...data, priceInUSDT });
    });
    
  } catch (error) {
    console.error("[DEX] 가격 업데이트 실패:", error);
  }
}

// ==================== Hyperliquid 데이터 ====================
async function initSpotMetadata(): Promise<string[]> {
  try {
    const response = await fetch("https://api.hyperliquid.xyz/info", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type: "spotMeta" }),
    });
    
    if (!response.ok) return [];
    
    const spotMetadata: any = await response.json();
    const tokenMap = new Map<number, string>();
    
    spotMetadata.tokens.forEach((token: any) => 
      tokenMap.set(token.index, token.name)
    );

    const spotCoins: string[] = [];
    spotMetadata.universe.forEach((pair: any) => {
      if (pair.name.startsWith("@")) {
        spotCoins.push(pair.name);
        const tokenNames = pair.tokens.map(
          (tokenIndex: number) => tokenMap.get(tokenIndex) || `Token${tokenIndex}`
        );
        if (tokenNames.length === 2) {
          const displayName = `${tokenNames[0]}-${tokenNames[1]}`;
          spotSymbolMapping.set(pair.name, displayName);
          reverseLookup.set(displayName, pair.name);
        }
      }
    });
    
    return spotCoins;
  } catch (error) {
    console.error("[HL] 스팟 메타데이터 로드 실패:", error);
    return [];
  }
}

function processOrderbookData(coin: string, l2Data: any): CoinOrderbook | null {
  if (!isRunning) return null;
  
  try {
    const displaySymbol = spotSymbolMapping.get(coin) || coin;
    let midPrice = targetMidsCache.get(coin);
    
    const bids = l2Data.levels[0] || [];
    const asks = l2Data.levels[1] || [];

    if (bids.length === 0 || asks.length === 0) return null;

    const bestBid = parseFloat(bids[0].px);
    const bestAsk = parseFloat(asks[0].px);
    
    if (!midPrice || midPrice === "0") {
      midPrice = ((bestBid + bestAsk) / 2).toString();
    }
    
    const spread = bestAsk - bestBid;
    const spreadPercentage = bestBid > 0 ? ((spread / bestBid) * 100).toFixed(4) : "0";
    const timestamp = new Date().toISOString();

    const maxLevels = 5;
    let bidCumulative = 0;
    const topBids: OrderbookLevel[] = bids.slice(0, maxLevels).map((bid: any, idx: number) => {
      bidCumulative += parseFloat(bid.sz);
      return {
        level: idx + 1,
        price: bid.px,
        size: bid.sz,
        cumulative: bidCumulative.toFixed(4),
        timestamp,
      };
    });

    let askCumulative = 0;
    const topAsks: OrderbookLevel[] = asks.slice(0, maxLevels).map((ask: any, idx: number) => {
      askCumulative += parseFloat(ask.sz);
      return {
        level: idx + 1,
        price: ask.px,
        size: ask.sz,
        cumulative: askCumulative.toFixed(4),
        timestamp,
      };
    });

    return {
      symbol: coin,
      displaySymbol,
      lastUpdate: timestamp,
      midPrice,
      spread: spread.toFixed(8),
      spreadPercentage: `${spreadPercentage}%`,
      bestBid,
      bestAsk,
      bids: topBids,
      asks: topAsks,
    };
  } catch (error) {
    console.error("[HL] 오더북 처리 오류:", error);
    return null;
  }
}

// ==================== 가격 임팩트 계산 ====================
function calcOrderbookImpact(
  levels: OrderbookLevel[],
  side: 'BUY' | 'SELL',
  notionalQuote: string,
  takerFeeBps = 0
): { avgExecPrice: Decimal; priceImpactPct: Decimal; feasible: boolean } {
  try {
    const wantQuote = new Decimal(notionalQuote);
    let remainingQuote = wantQuote;
    let filledBase = new Decimal(0);
    let filledQuote = new Decimal(0);

    for (const level of levels) {
      if (remainingQuote.lte(0)) break;

      const px = new Decimal(level.price);
      const qty = new Decimal(level.size);
      const levelQuote = qty.mul(px);

      if (levelQuote.gte(remainingQuote)) {
        const quoteTaken = remainingQuote;
        const baseTaken = quoteTaken.div(px);
        filledQuote = filledQuote.plus(quoteTaken);
        filledBase = filledBase.plus(baseTaken);
        remainingQuote = new Decimal(0);
      } else {
        filledQuote = filledQuote.plus(levelQuote);
        filledBase = filledBase.plus(qty);
        remainingQuote = remainingQuote.minus(levelQuote);
      }
    }

    if (remainingQuote.gt(0)) {
      return { avgExecPrice: new Decimal(0), priceImpactPct: new Decimal(Infinity), feasible: false };
    }

    const feeMult = new Decimal(1).plus(new Decimal(takerFeeBps).div(10_000));
    const totalQuoteWithFee = filledQuote.mul(feeMult);
    const avgExecPrice = totalQuoteWithFee.div(filledBase);
    const topOfBook = new Decimal(levels[0].price);
    const priceImpactPct = avgExecPrice.minus(topOfBook).div(topOfBook).mul(100);

    return {
      avgExecPrice,
      priceImpactPct,
      feasible: priceImpactPct.abs().lt(10)
    };
  } catch (error) {
    console.error("[HL] 가격 임팩트 계산 오류:", error);
    return { avgExecPrice: new Decimal(0), priceImpactPct: new Decimal(Infinity), feasible: false };
  }
}

function calculateOrderbookPriceImpact(
  orderbook: CoinOrderbook,
  side: "BUY" | "SELL",
  targetAmountUSD: number
): PriceImpact {
  const levels = side === "BUY" ? orderbook.asks : orderbook.bids;
  
  const result = calcOrderbookImpact(
    levels,
    side,
    targetAmountUSD.toString(),
    side === "BUY" ? HYPERLIQUID_FEE_BPS : 0
  );

  return {
    protocol: "Hyperliquid",
    side,
    targetAmount: targetAmountUSD,
    executionPrice: Number(result.avgExecPrice),
    priceImpactPercent: Number(result.priceImpactPct),
    slippage: Math.abs(Number(result.priceImpactPct)),
    feasible: result.feasible
  };
}

// Add new DEX price impact calculation
function calculateDexPriceImpact(
  dexData: DexPoolData,
  side: "BUY" | "SELL",
  targetAmountUSD: number
): PriceImpact {
  try {
    const pool = poolFromDexData(dexData);
    if (!pool) throw new Error("Invalid pool data");

    const quoteIsToken0 = dexData.token0Symbol === "USDT0" || dexData.token0Symbol === "USDT";
    const quoteToken = quoteIsToken0 ? pool.token0 : pool.token1;
    const baseToken = quoteIsToken0 ? pool.token1 : pool.token0;

    const price = dexData.priceInUSDT;
    const amountInQuoteUSD = side === "BUY" ? targetAmountUSD : targetAmountUSD * (1 / price);
    const amountInRaw = JSBI.BigInt(Math.floor(amountInQuoteUSD * 10 ** quoteToken.decimals));
    const amountIn = CurrencyAmount.fromRawAmount(quoteToken, amountInRaw);

    const route = new Route([pool], quoteToken, baseToken);
    const trade = Trade.fromRoute(route, amountIn, TradeType.EXACT_INPUT);

    const feePct = UPHEAVAL_FEE_BPS / 10_000;
    const execPrice = parseFloat(trade.executionPrice.toSignificant(12))
      * (side === "BUY" ? (1 + feePct) : (1 - feePct));

    return {
      protocol: "Upheaval",
      side,
      targetAmount: targetAmountUSD,
      executionPrice: execPrice,
      priceImpactPercent: parseFloat(trade.priceImpact.toSignificant(6)),
      slippage: parseFloat(trade.priceImpact.toSignificant(6)),
      feasible: true,
    };
  } catch (err) {
    console.error("[DEX] 정확 슬리피지 계산 실패 → fallback 사용:", err);
    
    const feePct = UPHEAVAL_FEE_BPS / 10_000;
    const execPrice = dexData.priceInUSDT * (side === "BUY" ? 1 + feePct : 1 - feePct);

    return {
      protocol: "Upheaval",
      side,
      targetAmount: targetAmountUSD,
      executionPrice: execPrice,
      priceImpactPercent: feePct * 100,
      slippage: feePct * 100,
      feasible: true,
    };
  }
}

// ==================== 골든 섹션 서치 ====================
function goldenSectionSearch(
  f: (x: number) => number,
  lo = 50,
  hi = Math.min(5_000, HL_MAX_NOTIONAL),
  tol?: number
): { size: number; value: number } {
  const tolerance = tol ?? Math.max(50, 0.02 * hi);
  const phi = 0.618;
  let a = lo;
  let b = hi;
  let c = b - (b - a) * phi;
  let d = a + (b - a) * phi;
  
  const maxIterations = 100;
  let iterations = 0;
  
  while (b - a > tolerance && iterations < maxIterations) {
    const fc = f(c);
    const fd = f(d);
    
    if (!isFinite(fc) && !isFinite(fd)) {
      break;
    } else if (!isFinite(fc)) {
      a = c;
      c = d;
      d = a + (b - a) * phi;
    } else if (!isFinite(fd)) {
      b = d;
      d = c;
      c = b - (b - a) * phi;
    } else {
      if (fc > fd) {
        a = c;
        c = d;
        d = a + (b - a) * phi;
      } else {
        b = d;
        d = c;
        c = b - (b - a) * phi;
      }
    }
    
    iterations++;
  }
  
  const size = (a + b) / 2;
  const value = f(size);
  
  return { 
    size, 
    value: isFinite(value) ? value : Number.NEGATIVE_INFINITY 
  };
}

function getBestLevelCapacityUSD(orderbook: CoinOrderbook, side: "BUY" | "SELL"): number {
  const level = side === "BUY" ? orderbook.asks[0] : orderbook.bids[0];
  if (!level) return 0;
  const px = parseFloat(level.price);
  const sz = parseFloat(level.size);
  return px * sz;
}

function calculateOrderbookPriceImpactOneLevel(
  orderbook: CoinOrderbook,
  side: "BUY" | "SELL",
  amountUSD: number
): PriceImpact | null {
  const level = side === "BUY" ? orderbook.asks[0] : orderbook.bids[0];
  if (!level) return null;

  const px = parseFloat(level.price);
  const capUSD = px * parseFloat(level.size);

  if (amountUSD > capUSD) {
    return {
      protocol: "Hyperliquid",
      side,
      targetAmount: amountUSD,
      executionPrice: 0,
      priceImpactPercent: 0,
      slippage: 0,
      feasible: false,
    };
  }

  return {
    protocol: "Hyperliquid",
    side,
    targetAmount: amountUSD,
    executionPrice: px,
    priceImpactPercent: 0,
    slippage: 0,
    feasible: true,
  };
}

// Replace the existing findOptimalTradeSizeGolden function
function findOptimalTradeSizeGolden(
  hlOrderbook: CoinOrderbook | null,
  dexData: DexPoolData | null,
  direction: "HL_TO_DEX" | "DEX_TO_HL"
): { amount: number; netProfit: number } {
  if (!hlOrderbook || !dexData) return { amount: 0, netProfit: -999 };

  // 베스트 레벨의 용량 계산
  const hlBestBidCapUSD = getBestLevelCapacityUSD(hlOrderbook, "SELL");
  const hlBestAskCapUSD = getBestLevelCapacityUSD(hlOrderbook, "BUY");

  // 방향에 따라 최대 주문 가능 금액 결정
  const maxOrderSize = direction === "HL_TO_DEX" 
    ? Math.min(hlBestAskCapUSD, HL_MAX_NOTIONAL)
    : Math.min(hlBestBidCapUSD, HL_MAX_NOTIONAL);

  // 주문 가능 금액이 너무 작으면 중단
  if (maxOrderSize < 50) {
    return { amount: maxOrderSize, netProfit: -999 };
  }

  // 첫 호가에서의 가격 영향도 계산
  let buyImpact: PriceImpact | null;
  let sellImpact: PriceImpact | null;

  if (direction === "HL_TO_DEX") {
    buyImpact = calculateOrderbookPriceImpactOneLevel(hlOrderbook, "BUY", maxOrderSize);
    sellImpact = calculateDexPriceImpact(dexData, "SELL", maxOrderSize);
  } else {
    buyImpact = calculateDexPriceImpact(dexData, "BUY", maxOrderSize);
    sellImpact = calculateOrderbookPriceImpactOneLevel(hlOrderbook, "SELL", maxOrderSize);
  }

  if (!buyImpact?.feasible || !sellImpact?.feasible) {
    return { amount: maxOrderSize, netProfit: -999 };
  }

  // 순수익 계산 (수수료 포함)
  const gross = ((sellImpact.executionPrice - buyImpact.executionPrice) / buyImpact.executionPrice) * 100;
  const netProfit = gross - (HYPERLIQUID_FEE_BPS + UPHEAVAL_FEE_BPS) * 0.01;

  return { 
    amount: Math.floor(maxOrderSize), // 정수로 내림
    netProfit: netProfit 
  };
}

// ==================== 모든 경로 분석 ====================
function analyzeAllRoutes(): ArbitrageRoute[] {
  if (!isRunning) return [];

  const routes: ArbitrageRoute[] = [];
  const validOrderbooks = incrementalOrderbooks.getAllValid();

  for (const token of TARGET_SYMBOLS) {
    let hlOrderbook: CoinOrderbook | null = null;
    
    for (const [symbol, orderbook] of validOrderbooks.entries()) {
      const baseToken = orderbook.displaySymbol.split("-")[0];
      if (baseToken === token) {
        hlOrderbook = orderbook;
        break;
      }
    }

    const dexData = dexPrices.get(token);

    // 경로 1: HL 매수 -> DEX 매도
    if (hlOrderbook && dexData) {
      const optimal = findOptimalTradeSizeGolden(hlOrderbook, dexData, "HL_TO_DEX");
      const testAmount = optimal.amount;
      
      const buyImpact = calculateOrderbookPriceImpact(hlOrderbook, "BUY", testAmount);
      const sellImpact = calculateDexPriceImpact(dexData, "SELL", testAmount);
      
      const hlBuyPrice = parseFloat(hlOrderbook.asks[0]?.price || "0");
      const dexSellPrice = dexData.priceInUSDT;
      
      let route: ArbitrageRoute = {
        token,
        direction: "HL->DEX",
        buyProtocol: "Hyperliquid",
        sellProtocol: "Upheaval",
        buyPrice: hlBuyPrice,
        sellPrice: dexSellPrice,
        priceGap: hlBuyPrice > 0 ? ((dexSellPrice - hlBuyPrice) / hlBuyPrice) * 100 : 0,
        optimalAmount: testAmount,
        netProfit: -999,
        estimatedProfitUSD: 0,
        buyPriceImpact: buyImpact?.slippage || 0,
        sellPriceImpact: sellImpact?.slippage || 0,
        buyExecutionPrice: buyImpact?.executionPrice || 0,
        sellExecutionPrice: sellImpact?.executionPrice || 0,
        status: "NO_DATA",
      };
      
      if (buyImpact?.feasible && sellImpact?.feasible) {
        const grossReturn = ((sellImpact.executionPrice - buyImpact.executionPrice) / buyImpact.executionPrice) * 100;
        route.netProfit = grossReturn - (HYPERLIQUID_FEE_BPS + UPHEAVAL_FEE_BPS) * 0.01;
        route.estimatedProfitUSD = (testAmount * route.netProfit) / 100;
        route.status = route.netProfit > 0 ? "PROFITABLE" : "NEGATIVE";
      }
      
      routes.push(route);
    } else {
      routes.push({
        token,
        direction: "HL->DEX",
        buyProtocol: "Hyperliquid",
        sellProtocol: "Upheaval",
        buyPrice: 0,
        sellPrice: 0,
        priceGap: 0,
        optimalAmount: 0,
        netProfit: -999,
        estimatedProfitUSD: 0,
        buyPriceImpact: 0,
        sellPriceImpact: 0,
        buyExecutionPrice: 0,
        sellExecutionPrice: 0,
        status: "NO_DATA",
      });
    }

    // 경로 2: DEX 매수 -> HL 매도
    if (hlOrderbook && dexData) {
      const optimal = findOptimalTradeSizeGolden(hlOrderbook, dexData, "DEX_TO_HL");
      const testAmount = optimal.amount;
      
      const buyImpact = calculateDexPriceImpact(dexData, "BUY", testAmount);
      const sellImpact = calculateOrderbookPriceImpact(hlOrderbook, "SELL", testAmount);
      
      let route: ArbitrageRoute = {
        token,
        direction: "DEX->HL",
        buyProtocol: "Upheaval",
        sellProtocol: "Hyperliquid",
        buyPrice: dexData.priceInUSDT,
        sellPrice: hlOrderbook.bestBid,
        priceGap: ((hlOrderbook.bestBid - dexData.priceInUSDT) / dexData.priceInUSDT) * 100,
        optimalAmount: testAmount,
        netProfit: -999,
        estimatedProfitUSD: 0,
        buyPriceImpact: buyImpact?.slippage || 0,
        sellPriceImpact: sellImpact?.slippage || 0,
        buyExecutionPrice: buyImpact?.executionPrice || 0,
        sellExecutionPrice: sellImpact?.executionPrice || 0,
        status: "NO_DATA",
      };
      
      if (buyImpact?.feasible && sellImpact?.feasible) {
        const grossReturn = ((sellImpact.executionPrice - buyImpact.executionPrice) / buyImpact.executionPrice) * 100;
        route.netProfit = grossReturn - (HYPERLIQUID_FEE_BPS + UPHEAVAL_FEE_BPS) * 0.01;
        route.estimatedProfitUSD = (testAmount * route.netProfit) / 100;
        route.status = route.netProfit > 0 ? "PROFITABLE" : "NEGATIVE";
      }
      
      routes.push(route);
    } else {
      routes.push({
        token,
        direction: "DEX->HL",
        buyProtocol: "Upheaval",
        sellProtocol: "Hyperliquid",
        buyPrice: 0,
        sellPrice: 0,
        priceGap: 0,
        optimalAmount: 0,
        netProfit: -999,
        estimatedProfitUSD: 0,
        buyPriceImpact: 0,
        sellPriceImpact: 0,
        buyExecutionPrice: 0,
        sellExecutionPrice: 0,
        status: "NO_DATA",
      });
    }
  }

  return routes;
}

// ==================== 실시간 디스플레이 ====================
function displayRealTimeRoutes(routes: ArbitrageRoute[]): void {
  process.stdout.write('\x1Bc');
  
  const now = new Date().toLocaleTimeString();
  
  console.log("=".repeat(105));
  console.log(`실시간 차익거래 모니터링 - ${now}`);
  console.log(`수수료: HL ${HYPERLIQUID_FEE_BPS}% | DEX ${UPHEAVAL_FEE_BPS}% (슬리피지는 실행가에 반영됨)`);
  console.log("=".repeat(105));
  
  console.log(
    "토큰".padEnd(6) +
    "방향".padEnd(8) +
    "B->S".padEnd(8) +
    "매수가".padEnd(12) +
    "실행가".padEnd(12) +
    "매도가".padEnd(12) +
    "실행가".padEnd(12) +
    "갭%".padEnd(9) +
    "순익%".padEnd(9) +
    "상태"
  );
  console.log("-".repeat(105));
  
  routes.forEach(route => {
    let statusIcon = "";
    let statusColor = "";
    
    if (route.status === "PROFITABLE") {
      statusIcon = "🟢";
      statusColor = "\x1b[32m";
    } else if (route.status === "NEGATIVE") {
      statusIcon = "🔴";
      statusColor = "\x1b[31m";
    } else {
      statusIcon = "⚪";
      statusColor = "\x1b[33m";
    }
    
    const resetColor = "\x1b[0m";
    const protoString = route.buyProtocol === "Hyperliquid" ? "HL->UH" : "UH->HL";
    const gapColor = route.priceGap >= 0 ? "\x1b[32m" : "\x1b[31m";
    
    console.log(
      route.token.padEnd(6) +
      route.direction.slice(0,6).padEnd(8) +
      protoString.padEnd(8) +
      `${route.buyPrice.toFixed(5)}`.padEnd(12) +
      `${route.buyExecutionPrice.toFixed(5)}`.padEnd(12) +
      `${route.sellPrice.toFixed(5)}`.padEnd(12) +
      `${route.sellExecutionPrice.toFixed(5)}`.padEnd(12) +
      gapColor + `${route.priceGap.toFixed(2)}%`.padEnd(9) + resetColor +
      statusColor + `${route.netProfit > -999 ? route.netProfit.toFixed(2) : 'N/A'}%`.padEnd(9) + resetColor +
      statusColor + statusIcon + resetColor
    );
  });
  
  console.log("=".repeat(105));
  
  const profitable = routes.filter(r => r.status === "PROFITABLE");
  const negative = routes.filter(r => r.status === "NEGATIVE");
  const noData = routes.filter(r => r.status === "NO_DATA");
  
  console.log(`📊 수익 가능: ${profitable.length} | 손실: ${negative.length} | 데이터 없음: ${noData.length} | 총: ${routes.length}`);
  
  if (profitable.length > 0) {
    console.log('\n💰 수익 가능 경로 상세:');  // Fix: Changed template literal to single quotes
    profitable.forEach(r => {
      console.log(`   ${r.token} ${r.direction}: ${r.netProfit.toFixed(3)}% (≈$${r.estimatedProfitUSD.toFixed(2)}) @ $${r.optimalAmount.toFixed(0)}`);
    });
  }

  console.log('\n📈 DEX 가격 정보:');  // Fix: Changed template literal to single quotes
  dexPrices.forEach((data, token) => {
    const liq = parseFloat(data.liquidity || '0');
    console.log(`   ${token}: $${data.priceInUSDT.toFixed(6)} (유동성: ${liq.toExponential(2)})`);
  });
  
  console.log("=".repeat(105));
}

// ==================== WebSocket 연결 ====================
let reconnectAttempts = 0;

async function connectWebSocket(spotCoins: string[]): Promise<void> {
  const ws = new WebSocket("wss://api.hyperliquid.xyz/ws");
  let reconnectTimeout: NodeJS.Timeout;
  let lastAnalysisTime = Date.now();
  const ANALYSIS_THROTTLE_MS = 3000;
  
  ws.binaryType = "arraybuffer";

  ws.on("open", () => {
    console.log("🔌 WebSocket 연결 성공");
    reconnectAttempts = 0;
    const targetCoins = spotCoins.filter(coin => {
      const displaySymbol = spotSymbolMapping.get(coin);
      if (!displaySymbol) return false;
      const baseToken = displaySymbol.split("-")[0];
      return TARGET_SYMBOLS.includes(baseToken);
    });
    
    console.log(`📡 구독 중: ${targetCoins.length}개 심볼`);
    targetCoins.forEach((coin) => {
      ws.send(JSON.stringify({ 
        method: "subscribe", 
        subscription: { type: "l2Book", coin } 
      }));
    });
  });

  ws.on("message", (data: Buffer) => {
    if (!isRunning) return;
    
    try {
      const message = JSON.parse(data.toString('utf8'));
      
      if (message.channel === "l2Book" && message.data) {
        const coin = message.data.coin;
        if (!coin.startsWith("@")) return;

        const coinData = processOrderbookData(coin, message.data);
        if (coinData) {
          incrementalOrderbooks.set(coinData.symbol, coinData);

          const now = Date.now();
          if (now - lastAnalysisTime >= ANALYSIS_THROTTLE_MS) {
            lastAnalysisTime = now;
            
            setImmediate(async () => {
              if (!isRunning) return;
              
              const validOrderbooks = incrementalOrderbooks.getAllValid();
              if (validOrderbooks.size > 0 && dexPrices.size > 0) {
                const routes = analyzeAllRoutes();
                displayRealTimeRoutes(routes);
                await appendToCSV(routes);
              }
            });
          }
        }
      }
    } catch (error) {
      console.error("[WS] 메시지 처리 오류:", error);
    }
  });

  ws.on("error", (error) => {
    console.error("❌ WebSocket 오류:", error);
  });
  
  ws.on("close", (code, reason) => {
    console.log(`🔌 WS 종료: ${code} - ${reason}`);
    if (isRunning) {
      const delay = Math.min(30_000, 1_000 * 2 ** reconnectAttempts++);
      console.log(`🔄 ${delay / 1000}s 후 재연결`);
      reconnectTimeout = setTimeout(() => connectWebSocket(spotCoins), delay);
    }
  });

  const intervals: NodeJS.Timeout[] = [];
  
  const midPriceInterval = setInterval(() => {
    if (!isRunning) return;
    updateTargetMidPricesOptimized().catch(console.error);
  }, 10000);
  intervals.push(midPriceInterval);

  const healthCheckInterval = setInterval(() => {
    if (!isRunning) return;
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  }, 30000);
  intervals.push(healthCheckInterval);

  process.on("SIGINT", () => {
    intervals.forEach(interval => clearInterval(interval));
    clearTimeout(reconnectTimeout);
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close();
    }
  });
}

// ==================== 시스템 상태 모니터링 ====================
function startSystemMonitoring(): void {
  const monitoringInterval = setInterval(() => {
    if (!isRunning) {
      clearInterval(monitoringInterval);
      return;
    }
    
    const memUsage = process.memoryUsage();
    const memUsageMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    
    if (memUsageMB > 200) {
      console.log(`⚠️  메모리 사용량 높음: ${memUsageMB}MB`);
    }
    
    const validOrderbooks = incrementalOrderbooks.getAllValid();
    const orderbookCount = validOrderbooks.size;
    const dexPriceCount = dexPrices.size;
    
    if (orderbookCount === 0 || dexPriceCount === 0) {
      console.log(`⚠️  데이터 부족: HL오더북=${orderbookCount}, DEX가격=${dexPriceCount}`);
    }
    
  }, 60000);
}

// ==================== Graceful Shutdown 처리 ====================
function setupGracefulShutdown(): void {
  const shutdown = async (signal: string) => {
    console.log(`\n🛑 ${signal} 신호 수신 - 안전하게 종료 중...`);
    isRunning = false;
    
    if (dexUpdateInterval) {
      clearInterval(dexUpdateInterval);
      console.log("✅ DEX 업데이트 인터벌 정리됨");
    }
    
    if (ttlSweepInterval) {
      clearInterval(ttlSweepInterval);
      console.log("✅ TTL 캐시 정리 인터벌 정리됨");
    }
    
    if (csvWriteStream) {
      await new Promise<void>((resolve) => {
        csvWriteStream!.end(() => {
          console.log("✅ CSV 파일 저장 완료");
          resolve();
        });
      });
    }
    
    setTimeout(() => {
      console.log("🔚 종료 완료");
      process.exit(0);
    }, 1000);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  
  process.on('uncaughtException', (error) => {
    console.error('❌ 처리되지 않은 예외:', error);
    shutdown("UNCAUGHT_EXCEPTION");
  });
  
  process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ 처리되지 않은 Promise 거부:', reason);
    shutdown("UNHANDLED_REJECTION");
  });
}

// ==================== 메인 함수 ====================
async function run(): Promise<void> {
  console.log('🚀 차익거래 봇 시작 중...\n');  // Fix: Changed template literal to single quotes

  setupGracefulShutdown();

  try {
    outputDir = path.join(process.cwd(), "arbitrage-optimized-analysis");
    await fs.mkdir(outputDir, { recursive: true });
    console.log(`📁 출력 디렉토리: ${outputDir}`);

    await initializeCSVStream();
    console.log("📝 CSV 스트림 초기화 완료");

    console.log("📊 Hyperliquid 메타데이터 로드 중...");
    const allSpotCoins = await initSpotMetadata();
    if (allSpotCoins.length === 0) {
      throw new Error("Hyperliquid 현물 코인을 찾을 수 없습니다");
    }
    console.log(`✅ ${allSpotCoins.length}개 현물 페어 로드됨`);

    const targetSpotCoins = allSpotCoins.filter((internalSymbol) => {
      const displayName = spotSymbolMapping.get(internalSymbol);
      if (!displayName) return false;
      const baseSymbol = displayName.split("-")[0];
      return TARGET_SYMBOLS.includes(baseSymbol);
    });

    if (targetSpotCoins.length === 0) {
      throw new Error(`목표 토큰 (${TARGET_SYMBOLS.join(", ")})에 해당하는 페어를 찾을 수 없습니다`);
    }
    console.log(`🎯 타겟 페어 ${targetSpotCoins.length}개 발견:`, 
      targetSpotCoins.map(s => spotSymbolMapping.get(s)).join(", "));

    console.log("💰 초기 가격 데이터 로드 중...");
    await Promise.all([
      updateTargetMidPricesOptimized(),
      updateAllDexPrices()
    ]);
    console.log(`✅ HL Mid가격: ${targetMidsCache.size}개, DEX 가격: ${dexPrices.size}개 로드됨`);

    dexUpdateInterval = setInterval(async () => {
      if (!isRunning) return;
      await updateAllDexPrices();
    }, 5000);

    ttlSweepInterval = setInterval(() => {
      if (!isRunning) return;
      incrementalOrderbooks.sweep();
    }, 10000);

    startSystemMonitoring();
    
    console.log("🔌 WebSocket 연결 시작...");
    await connectWebSocket(targetSpotCoins);
    
  } catch (error) {
    console.error("❌ 초기화 실패:", error);
    process.exit(1);
  }
}

// ==================== 프로그램 시작 ====================
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const isMainModule = process.argv[1] === __filename || process.argv[1]?.endsWith('collect-orderbooks.ts');

if (isMainModule) {
  run().catch((error) => {
    console.error("❌ 프로그램 실행 오류:", error);
    process.exit(1);
  });
}

export {
  run,
  analyzeAllRoutes,
  calculateOrderbookPriceImpact,
  calculateDexPriceImpact,
  goldenSectionSearch,
  findOptimalTradeSizeGolden,
  TTLMap,
  TARGET_SYMBOLS,
  HYPERLIQUID_FEE_BPS,
  UPHEAVAL_FEE_BPS
};
