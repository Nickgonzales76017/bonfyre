/* Generated from architecture/economic/settlement.contract.yaff.
 *
 * Do not edit. Change the contract and re-run
 * cmd/BonfyreAtlas/bonfyre-atlas compile-contracts settlement; the surface test fails if this
 * header and the contract disagree.
 *
 * Cannot infer: a declared rail -> a configured one. a declared settlement rail -> a configured one, a settled payment, or a reserved organ
 */
#ifndef BF_SETTLEMENT_H
#define BF_SETTLEMENT_H

#define BF_SETTLEMENT_RAIL      "x402"
#define BF_SETTLEMENT_QUOTE     "/mpp/quote"
#define BF_SETTLEMENT_DISCOVERY "/capability"

/* Every grammar that quotes a price renders the declaration above. */
#define BF_SETTLEMENT_CARRIERS  { "x402", "MPP", "UCP", "ACP", "Beckn" }
#define BF_SETTLEMENT_CARRIER_N 5

#endif /* BF_SETTLEMENT_H */
