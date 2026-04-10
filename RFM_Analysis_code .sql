-- ═══════════════════════════════════════════════════════════════
-- STEP 1 │ BASE AGGREGATION
--         Contract Age, Recency, Frequency, Monetary per customer
-- ═══════════════════════════════════════════════════════════════
WITH RFM AS (
    SELECT
        cr.id,
        CASE
            WHEN DATEDIFF(year, created_date, '2022-09-01') < 1 THEN 1.0
            ELSE      DATEDIFF(year, created_date, '2022-09-01')
        END                                                             AS Contract_age,
        DATEDIFF(day, MAX(ct.Purchase_Date), '2022-09-01')             AS Recency,
        ROUND(
            COUNT(ct.Transaction_ID) /
            CASE
                WHEN DATEDIFF(year, created_date, '2022-09-01') < 1 THEN 1.0
                ELSE      DATEDIFF(year, created_date, '2022-09-01')
            END
        , 2)                                                            AS Frequency,
        SUM(ct.GMV)                                                     AS Monetary
    FROM       Customer_Registered  cr
    JOIN       Customer_Transaction ct  ON  cr.id = ct.CustomerID
                                        AND ct.GMV > 0
    WHERE  created_date IS NOT NULL                                 
    GROUP BY   cr.id, cr.created_date
),
-- ═══════════════════════════════════════════════════════════════
-- STEP 2 │ IQR THRESHOLDS
--         Compute Q1 / Q2 / Q3 / Max for each R, F, M metric
--         PERCENTILE_CONT runs across the full dataset via OVER()
-- ═══════════════════════════════════════════════════════════════
IQR AS (
    SELECT id, Contract_age, Recency, Frequency, Monetary,
        -- PERCENTILE_DISC returns an ACTUAL value that exists in the data
        -- It picks the first real data point AT OR ABOVE the percentile
        -- This prevents Q3 from equalling MAX in skewed distributions
        PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY Recency)   OVER() AS R_Q1,
        PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY Recency)   OVER() AS R_Q2,
        PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY Recency)   OVER() AS R_Q3,
        MAX(Recency)                                            OVER() AS R_Q4,
        PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY Frequency) OVER() AS F_Q1,
        PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY Frequency) OVER() AS F_Q2,
        PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY Frequency) OVER() AS F_Q3,
        MAX(Frequency)                                          OVER() AS F_Q4,
        PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY Monetary)  OVER() AS M_Q1,
        PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY Monetary)  OVER() AS M_Q2,
        PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY Monetary)  OVER() AS M_Q3,
        MAX(Monetary)                                           OVER() AS M_Q4
    FROM RFM
),
-- ═══════════════════════════════════════════════════════════════
-- STEP 3 │ RFM SCORING
--         R : inverted  — fewer days since purchase = score 4
--         F : ascending — higher frequency           = score 4
--         M : ascending — higher monetary            = score 4
-- ═══════════════════════════════════════════════════════════════
RFM_RANK AS (
    SELECT
        id,
        CASE
            WHEN Recency   <= R_Q1 THEN 4
            WHEN Recency   <= R_Q2 THEN 3
            WHEN Recency   <= R_Q3 AND R_Q3 < R_Q4 THEN 2
            ELSE                        1
        END AS R_Score,
        CASE
            WHEN Frequency <= F_Q1 THEN 1
            WHEN Frequency <= F_Q2 THEN 2
            WHEN Frequency <= F_Q3 AND F_Q3 < F_Q4 THEN 3
            ELSE                        4
        END AS F_Score,
        CASE
            WHEN Monetary  <= M_Q1 THEN 1
            WHEN Monetary  <= M_Q2 THEN 2
            WHEN Monetary  <= M_Q3 AND M_Q3 < M_Q4 THEN 3
            ELSE                        4
        END AS M_Score
    FROM IQR
),
-- ═══════════════════════════════════════════════════════════════
-- STEP 4 │ COMBINE SCORES
--         Join scores back to metrics, build RFM code string
-- ═══════════════════════════════════════════════════════════════
RFM_FINAL AS (
    SELECT
        i.id,
        i.Contract_age,
        i.Recency,
        i.Frequency,
        i.Monetary,
        r.R_Score,
        r.F_Score,
        r.M_Score,
        CONCAT(r.R_Score, r.F_Score, r.M_Score) AS RFM_Score
    FROM       RFM_RANK r
    JOIN       IQR      i   ON  r.id = i.id
)
-- ═══════════════════════════════════════════════════════════════
-- STEP 5 │ FINAL OUTPUT
--         Map RFM code to BCG Segmentation via lookup table
-- ═══════════════════════════════════════════════════════════════
SELECT
    rf.id,
    rf.Contract_age,
    rf.Recency,
    rf.Frequency,
    rf.Monetary,
    rf.RFM_Score,
    rm.Segmentation
FROM       RFM_FINAL  rf
JOIN       RFM_Mapping rm   ON  rf.RFM_Score = rm.RFM_Score
ORDER BY Segmentation desc
