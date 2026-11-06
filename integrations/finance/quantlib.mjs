// QuantLib provider boundary for source-priced payment alternatives. No ledger writes.
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { resolve, join } from 'node:path';

// Python is only the upstream SWIG ABI here; QuantLib's compiled C++ owns the math.
const upstream = String.raw`
import sys,json,math
import QuantLib as q
x=json.load(sys.stdin)
if q.__version__!='1.43': raise ValueError('Expected pinned QuantLib 1.43')
y,m,d=map(int,x['reference_date'].split('-'))
asof=q.Date(d,m,y)
dc=q.Actual365Fixed()
answer=[]
for item in x['alternatives']:
    n=item['months']
    schedule=q.Schedule(asof,asof+q.Period(n,q.Months),q.Period(1,q.Months),q.NullCalendar(),q.Unadjusted,q.Unadjusted,q.DateGeneration.Forward,False)
    dates=list(schedule)[:-1] if item['billing_timing']=='advance' else list(schedule)[1:]
    leg=q.Leg([q.SimpleCashFlow(float(item['periodic_amount']),date) for date in dates])
    price=float(item['upfront_amount'])
    rate=q.CashFlows.yieldRate(leg,price,dc,q.Compounded,q.Annual,True,asof,asof,1e-12,10000,0.1)
    present=q.CashFlows.npv(leg,rate,dc,q.Compounded,q.Annual,True,asof,asof)
    duration=q.CashFlows.duration(leg,rate,dc,q.Compounded,q.Annual,q.Duration.Modified,True,asof,asof)
    if not all(math.isfinite(v) for v in [rate,present,duration]): raise ValueError('Nonfinite result')
    if abs(present-price)>0.000001: raise ValueError('Cashflow repricing mismatch')
    answer.append({'id':item['id'],'effective_annual_indifference_rate':rate,'repriced_upfront_amount':format(present,'.8f'),'repricing_error':present-price,'modified_duration_years':duration,'local_pv_change_per_basis_point':-duration*present*0.0001,'payment_dates':[date.ISO() for date in dates],'calculation':'QuantLib CashFlows.yieldRate / npv / duration','billing_timing':item['billing_timing'],'source_refs':item['source_refs']})
print(json.dumps({'provider':'QuantLib','version':q.__version__,'day_count':dc.name(),'date_adjustment':'Unadjusted subscription anniversaries; no bank-calendar rule supplied','results':answer}))
`;

export function comparePaymentOptions(input, options = {}) {
  if (!input || input.schema !== 'aurekai.payment-option-comparison.v1') throw Error('Unsupported payment comparison');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(input.reference_date ?? '')) throw Error('Missing reference date');
  if (!/^[A-Z]{3}$/.test(input.currency ?? '')) throw Error('One explicit currency is required');
  if (!Array.isArray(input.alternatives) || !input.alternatives.length || input.alternatives.length > 64) throw Error('Supply 1–64 alternatives');
  const names = new Set();
  for (const row of input.alternatives) {
    if (!row || typeof row.id !== 'string' || !row.id || names.has(row.id)) throw Error('Duplicate or absent comparison identity');
    names.add(row.id);
    if (!Number.isSafeInteger(row.months) || row.months < 2 || row.months > 120) throw Error('Invalid payment count');
    if (!['advance', 'arrears'].includes(row.billing_timing)) throw Error('State billing timing explicitly');
    for (const key of ['upfront_amount', 'periodic_amount']) {
      if (typeof row[key] !== 'string' || !/^\d{1,10}(?:\.\d{1,2})?$/.test(row[key]) || Number(row[key]) <= 0) throw Error('A positive source price is required');
    }
    if (row.billing_timing === 'advance' && Number(row.upfront_amount) <= Number(row.periodic_amount)) throw Error('No finite positive-price comparison for this timing');
    if (!Array.isArray(row.source_refs) || !row.source_refs.length || row.source_refs.some(ref => typeof ref !== 'string' || !ref)) throw Error('Missing price evidence');
  }
  const python = options.python ?? process.env.BONFYRE_QUANTLIB_PYTHON ?? join(homedir(), '.bonfyre/providers/quantlib-1.43/bin/python');
  if (!existsSync(python)) throw Error('QuantLib provider unavailable: ' + python);
  const run = spawnSync(python, ['-I', '-c', upstream], {
    input: JSON.stringify(input), encoding: 'utf8', timeout: 30000, maxBuffer: 2 * 1024 * 1024,
    env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: homedir() },
  });
  if (run.error || run.status !== 0) throw Error('QuantLib calculation failed: ' + (run.error?.message ?? run.stderr));
  const result = JSON.parse(run.stdout);
  if (result.provider !== 'QuantLib' || result.version !== '1.43' || result.results.length !== names.size) throw Error('Provider response mismatch');
  return {
    schema: 'aurekai.payment-option-result.v1', input_ref: input.input_ref ?? null,
    reference_date: input.reference_date, reference_date_basis: input.reference_date_basis,
    currency: input.currency, assumption_notice: input.assumption_notice ?? null,
    ...result, effect: 'analysis_only', debt_created: false, payment_sent: false,
  };
}

if (import.meta.main) {
  const [inputPath, outputPath] = process.argv.slice(2);
  if (!inputPath) throw Error('Usage: bun integrations/finance/quantlib.mjs input.json [output.json]');
  const raw = readFileSync(resolve(inputPath), 'utf8');
  if (Buffer.byteLength(raw) > 2 * 1024 * 1024) throw Error('Comparison input exceeds 2 MB');
  const result = comparePaymentOptions(JSON.parse(raw));
  const text = JSON.stringify(result, null, 2) + '\n';
  if (outputPath) writeFileSync(resolve(outputPath), text, { flag: 'wx', mode: 0o600 });
  console.log(text);
}
