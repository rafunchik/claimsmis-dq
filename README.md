# DQ Spark job



## Reference value checks

The reference values are stored in the gdp_dev database under the gdp_reference schema in the ref_harmonization table,
currently as global name -> local code value pairs.
Note that the reference values are cached in memory for performance reasons, and loaded once with the following query:

``` sql
select global_ref_data_name, loc_code from gdp_reference.ref_harmonization WHERE global_ref_data_name IN
('settlement_type', 'damage_specification', 'product_line', 'line_of_business', 'customer_segment', 'business_area',
'loss_type', 'claim_assessment_method', 'claim_status', 'claim_process_method', 'sales_channel')
```
              

## Reporting

The DQ app populates the gdp_dev database. Under the gdp_dq schema there are two tables,
dq_error, for all the failed check entries, and dq_submission_info, with information about 
the dataset being checked (OE, reporting date, etc.). 
Apart from the detailed view in dq_error the DQ dashboard uses the dq_aggregated materialized
view to display aggregated DQ results.

## Deployment:

Currently, it is a manual process:

- Increase the version in build.sbt
- Build the jar file using `sbt assembly`
- Copy the jar file to Databricks' dbfs once uploaded note the dbfs jar path
- Update the respective ADF pipeline to use the new jar path (just look for dbfs in the json, increase the jar version, commit and push) 