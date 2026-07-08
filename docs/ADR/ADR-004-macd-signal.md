# ADR-004: MACD Signal Calculation (EMA9 instead of SMA)

## Status
Accepted

## Context
The MACD indicator is composed of the MACD line (EMA12 - EMA26) and the Signal line. A previous approach might have approximated the Signal line using a Simple Moving Average (SMA) of the MACD line to simplify the SQL. However, this approximation risks failing the G-03 data quality test, as the true definition of the MACD Signal line is a 9-period Exponential Moving Average (EMA) of the MACD line.

## Decision
We decided to compute the true EMA9 for the MACD Signal line. To achieve this cleanly in SQL without massive nested queries, we introduced two intermediate ephemeral/view models:
1. `int_macd_line`: Calculates the MACD line.
2. `int_macd_signal`: Applies the EMA macro on the MACD line.

## Consequences
- **Positive:** The calculation is mathematically accurate and strictly follows the standard definition of MACD, ensuring G-03 tests pass and financial analysts can trust the output.
- **Negative:** Increases the complexity of the dbt DAG by adding intermediate models, slightly increasing transformation time.
