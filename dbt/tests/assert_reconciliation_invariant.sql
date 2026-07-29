-- Assert reconciliation invariant holds for all stored results.

SELECT *
FROM {{ ref('batch_reconciliation') }}
WHERE unexplained_difference <> 0
   OR invariant_ok = false
