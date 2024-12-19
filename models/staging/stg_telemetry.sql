{{ config(materialized='ephemeral') }}

select *
from {{source('QA_Lake','OBS_LAKE')}}