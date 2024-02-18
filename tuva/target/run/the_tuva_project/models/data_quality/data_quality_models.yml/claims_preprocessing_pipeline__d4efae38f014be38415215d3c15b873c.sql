select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

    select *
    from "synthea"."data_quality"."claims_preprocessing_test_detail"
    where pipeline_test = 1


      
    ) dbt_internal_test