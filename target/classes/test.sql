insert into budget_db.budget_import_data_detl
with tmp1 as (
    select 'M' as rank_flag, 'M' rank_code
    union all
    select 'Q' as rank_flag, 'Q' rank_code
    union all
    select 'MQ' as rank_flag, 'M' rank_code
    union all
    select 'MQ' as rank_flag, 'Q' rank_code
)
   , budget_change_task_detail as (
    SELECT CAST(parse_json(json_val) -> 'budgetChangeId' AS VARCHAR)     AS budget_change_id,
           CAST(parse_json(json_val) -> 'version' AS VARCHAR)            AS task_version,
           CAST(parse_json(json_val) -> 'changeType' AS VARCHAR)         AS change_type,
           CAST(parse_json(json_val) -> 'phmcFlag' AS VARCHAR)           AS phmc_flag,
           CAST(parse_json(json_val) -> 'phmcCode' AS VARCHAR)           AS phmc_code,
           CAST(parse_json(json_val) -> 'phmcName' AS VARCHAR)           AS phmc_name,
           CAST(parse_json(json_val) -> 'buisCode' AS VARCHAR)           AS buis_code,
           CAST(parse_json(json_val) -> 'buisName' AS VARCHAR)           AS buis_name,
           CAST(parse_json(json_val) -> 'channelType' AS VARCHAR)        AS channel_type,
           CAST(parse_json(json_val) -> 'syncGroupRankFlag' AS VARCHAR)  AS sync_group_rank_flag,
           CAST(parse_json(json_val) -> 'syncMngeRankFlag' AS VARCHAR)   AS sync_mnge_rank_flag,
           CAST(parse_json(json_val) -> 'syncSdAreaRankFlag' AS VARCHAR) AS sync_sd_area_rank_flag,
           CAST(parse_json(json_val) -> 'syncStrgRankFlag' AS VARCHAR)   AS sync_strg_rank_flag,
           CAST(parse_json(json_val) -> 'syncDistRankFlag' AS VARCHAR)   AS sync_dist_rank_flag,
           CAST(parse_json(json_val) -> 'syncPhmcRankFlag' AS VARCHAR)   AS sync_phmc_rank_flag,
           CAST(parse_json(json_val) -> 'yearMonth' AS VARCHAR)          AS task_month,
           CAST(parse_json(json_val) -> 'desc' AS VARCHAR)               AS change_desc,
           CAST(parse_json(json_val) -> 'saleTask' AS VARCHAR)           AS sale_task,
           CAST(parse_json(json_val) -> 'grosProfTask' AS VARCHAR)       AS gros_prof_task,
           CAST(parse_json(json_val) -> 'dataTypeFlag' AS VARCHAR)       AS data_type_flag,
           CAST(parse_json(json_val) -> 'opManCode' AS VARCHAR)          AS op_man_code,
           CAST(parse_json(json_val) -> 'opManName' AS VARCHAR)          AS op_man_name,
           CAST(parse_json(json_val) -> 'sendTime' AS VARCHAR)           AS send_time
    FROM budget_db.budget_import_ori_data
    where change_type = '0'
)
   , expand_detl as (
    select budget_change_id,
           buis_name,
           phmc_name,
           op_man_code,
           op_man_name,
           phmc_code,
           change_type,
           channel_type,
           phmc_flag,
           buis_code,
           'GROUP' sync_level,
           t2.rank_code,
           task_month,
           change_desc,
           sale_task,
           gros_prof_task,
           task_version
    from budget_change_task_detail t1
             join tmp1 t2
                  on t1.sync_group_rank_flag = t2.rank_flag
    union all
    select budget_change_id,
           buis_name,
           phmc_name,
           op_man_code,
           op_man_name,
           phmc_code,
           change_type,
           channel_type,
           phmc_flag,
           buis_code,
           'MNGE' sync_level,
           t2.rank_code,
           task_month,
           change_desc,

           sale_task,
           gros_prof_task,
           task_version
    from budget_change_task_detail t1
             join tmp1 t2
                  on t1.sync_mnge_rank_flag = t2.rank_flag
    union all
    select budget_change_id,
           buis_name,
           phmc_name,
           op_man_code,
           op_man_name,
           phmc_code,
           change_type,
           channel_type,
           phmc_flag,
           buis_code,
           'SD_AREA' sync_level,
           t2.rank_code,
           task_month,
           change_desc,

           sale_task,
           gros_prof_task,
           task_version
    from budget_change_task_detail t1
             join tmp1 t2
                  on t1.sync_sd_area_rank_flag = t2.rank_flag
    union all
    select budget_change_id,
           buis_name,
           phmc_name,
           op_man_code,
           op_man_name,
           phmc_code,
           change_type,
           channel_type,
           phmc_flag,
           buis_code,
           'STRG' sync_level,
           t2.rank_code,
           task_month,
           change_desc,

           sale_task,
           gros_prof_task,
           task_version
    from budget_change_task_detail t1
             join tmp1 t2
                  on t1.sync_strg_rank_flag = t2.rank_flag
    union all
    select budget_change_id,
           buis_name,
           phmc_name,
           op_man_code,
           op_man_name,
           phmc_code,
           change_type,
           channel_type,
           phmc_flag,
           buis_code,
           'DIST' sync_level,
           t2.rank_code,
           task_month,
           change_desc,

           sale_task,
           gros_prof_task,
           task_version
    from budget_change_task_detail t1
             join tmp1 t2
                  on t1.sync_dist_rank_flag = t2.rank_flag
    union all
    select budget_change_id,
           buis_name,
           phmc_name,
           op_man_code,
           op_man_name,
           phmc_code,
           change_type,
           channel_type,
           phmc_flag,
           buis_code,
           'PHMC' sync_level,
           t2.rank_code,
           task_month,
           change_desc,

           sale_task,

           gros_prof_task,
           task_version
    from budget_change_task_detail t1
             join tmp1 t2
                  on t1.sync_phmc_rank_flag = t2.rank_flag
)
select budget_change_id,
       uuid()                                                             as id,
       null                                                               as ref_budget_change_id,
       phmc_code,
       null                                                               as comp_code,
       task_version,
       null                                                               as comp_name,
       phmc_name,
       sync_level,
       rank_code,
       channel_type,
       null                                                               as sync_level_name,
       null                                                               as rank_name,
       null                                                               as channel_type_name,
       buis_code,
       buis_name,
       task_month,
       change_desc,
       JSON_OBJECT('saleTask', sale_task, 'grosProfTask', gros_prof_task) as json_val,
       op_man_code,
       op_man_name,
       CURRENT_TIMESTAMP()                                                   etl_time

from expand_detl t1



