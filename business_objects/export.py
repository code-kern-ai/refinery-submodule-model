from typing import List, Tuple, Any, Optional

from . import general
from .. import enums
from ..business_objects.payload import get_base_query_valid_labels_manual
from ..models import Attribute
from . import user_session
from ..util import prevent_sql_injection

OUTSIDE_CONSTANT = "OUTSIDE"


def build_full_record_sql_export(
    project_id: str, attributes: List[Attribute], user_session_id: str
) -> str:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    user_session_id = prevent_sql_injection(
        user_session_id, isinstance(user_session_id, str)
    )
    select_part: str = ""
    for att in attributes:
        if select_part != "":
            select_part += ", \n"
        select_part += f"r.data->> '{att.name}' AS \"{att.name}\""

    classification_part, select_add = __build_classification_part(project_id)

    if select_add:
        select_part += ",\n" + select_add

    user_session_part, user_session_order = __build_user_session_part(
        project_id, user_session_id
    )
    extraction_part, select_add = __build_extraction_part(project_id, user_session_part)

    if select_add:
        select_part += ",\n" + select_add
    return __get_final_sql(
        project_id,
        select_part,
        classification_part,
        extraction_part,
        user_session_part,
        user_session_order,
    )


def __build_user_session_part(project_id: str, user_session_id: str) -> Tuple[str, str]:
    if not user_session_id:
        return "", ""

    session = user_session.get(project_id, user_session_id)

    if session.random_seed:
        general.execute(f"SELECT setseed({session.random_seed});")

    sql = str(session.id_sql_statement)
    index = sql.rfind("ORDER BY")
    if index == -1:
        print("coundn't find ORDER BY -> full export instead")
        return "", ""
    sql, order_part = sql[:index], sql[index:]
    order_select = ""
    if order_part.rfind('"') != -1:
        order_parts = order_part.split('"', 3)
        order_column = '"' + '"'.join(order_parts[1:]).strip()
        order_select = '"' + order_parts[1].strip() + '"'
    else:
        order_parts = order_part.split(" ", 3)
        order_column = " ".join(order_parts[2:]).strip()
        order_select = order_parts[2].strip()

    sql = sql.replace(
        "SELECT id_grabber.record_id",
        f"SELECT id_grabber.record_id, {order_select}",
    )
    return sql, order_column


def __build_classification_part(project_id: str) -> Tuple[str, str]:
    classification_case = __get_case_classification(project_id)
    if not classification_case:
        return "", ""

    columns_str_agg = ""
    label_case = ""
    select_add = ""
    for classification_task in classification_case:
        if columns_str_agg:
            columns_str_agg += ","
        columns_str_agg += f"""
string_agg({classification_task.col_name},', ') FILTER (WHERE source_type = '{enums.LabelSource.MANUAL.value}' AND vri_rla_id IS NOT NULL) AS {classification_task.col_name[:-1]}__{enums.LabelSource.MANUAL.value}\",
string_agg({classification_task.col_name},', ') FILTER (WHERE source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}') AS {classification_task.col_name[:-1]}__{enums.LabelSource.WEAK_SUPERVISION.value}\",
string_agg(confidence::TEXT,', ' ORDER BY record_id) FILTER (WHERE source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}' AND {classification_task.col_name} IS NOT NULL ) AS {classification_task.col_name[:-1]}__{enums.LabelSource.WEAK_SUPERVISION.value}__confidence\""""
        if label_case:
            label_case += ",\n"
        label_case += classification_task.case_full
        if select_add:
            select_add += ",\n"
        select_add += f"""{classification_task.col_name[:-1]}__{enums.LabelSource.MANUAL.value}\",            
{classification_task.col_name[:-1]}__{enums.LabelSource.WEAK_SUPERVISION.value}\",
{classification_task.col_name[:-1]}__{enums.LabelSource.WEAK_SUPERVISION.value}__confidence\""""

    classification_sql = __get_classification_labels(
        project_id, columns_str_agg, label_case
    )

    return classification_sql, select_add


def __build_extraction_part(project_id: str, session_sql: str) -> Tuple[str, str]:
    extraction_case = __get_case_information_extraction(project_id)

    if not extraction_case:
        return "", ""

    token_limit = __get_max_token(project_id, session_sql)
    if not token_limit:
        raise Exception("No token limit found, was tokenization already completed?")

    columns_arr_agg = ""
    columns_case_att_id = ""
    columns_case_BI = ""
    columns_case_labeling_task = ""
    select_add = ""

    for extraction_task in extraction_case:
        if columns_arr_agg:
            columns_arr_agg += ","
        columns_arr_agg += f"""
    ARRAY_AGG({extraction_task.col_name} ORDER BY record_id, token_index) FILTER (WHERE {extraction_task.col_name} IS NOT NULL AND source_type = '{enums.LabelSource.MANUAL.value}') AS {extraction_task.col_name[:-1]}__{enums.LabelSource.MANUAL.value}\",
    ARRAY_AGG({extraction_task.col_name} ORDER BY record_id, token_index) FILTER (WHERE {extraction_task.col_name} IS NOT NULL AND source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}') AS {extraction_task.col_name[:-1]}__{enums.LabelSource.WEAK_SUPERVISION.value}\",
    ARRAY_AGG({extraction_task.col_name[:-1]}__confidence\"::float ORDER BY record_id, token_index) FILTER (WHERE {extraction_task.col_name[:-1]}__confidence\" IS NOT NULL AND source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}') AS {extraction_task.col_name[:-1]}__{enums.LabelSource.WEAK_SUPERVISION.value}__confidence\""""
        if columns_case_att_id:
            columns_case_att_id += ","
        columns_case_att_id += f"""
    CASE WHEN BIO_part.token_index IS NULL AND token_index.attribute_id = '{extraction_task.attribute_id}' THEN '{OUTSIDE_CONSTANT}' ELSE {extraction_task.col_name} END AS {extraction_task.col_name},
    CASE WHEN BIO_part.token_index IS NULL AND token_index.attribute_id = '{extraction_task.attribute_id}' AND token_index.source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}' THEN 0 WHEN {extraction_task.col_name} IS NULL THEN NULL ELSE confidence END AS {extraction_task.col_name[:-1]}__confidence\""""
        if columns_case_BI:
            columns_case_BI += ","
        columns_case_BI += f"""
    CASE is_beginning_token WHEN TRUE THEN 'B-' WHEN FALSE THEN 'I-' END || {extraction_task.col_name} AS {extraction_task.col_name}"""
        if columns_case_labeling_task:
            columns_case_labeling_task += ",\n"
        columns_case_labeling_task += extraction_task.case_task

        if select_add:
            select_add += ","
        select_add += f"""{extraction_task.col_name[:-1]}__{enums.LabelSource.MANUAL.value}\",
{extraction_task.col_name[:-1]}__{enums.LabelSource.WEAK_SUPERVISION.value}\",
{extraction_task.col_name[:-1]}__{enums.LabelSource.WEAK_SUPERVISION.value}__confidence\""""

    extraction_sql = __get_extraction_labels_BIO(
        project_id,
        token_limit,
        columns_arr_agg,
        columns_case_att_id,
        columns_case_BI,
        columns_case_labeling_task,
        session_sql,
    )

    return extraction_sql, select_add


def __get_final_sql(
    project_id: str,
    select_part: str,
    classification_part: str,
    extraction_part: str,
    user_session_part: str = "",
    order_by_part: str = "",
) -> str:
    base_query_manual = get_base_query_valid_labels_manual(project_id)
    select_part = select_part.replace("\n", "\n        ")
    classification_part = classification_part.replace("\n", "\n        ")
    extraction_part = extraction_part.replace("\n", "\n        ")

    if classification_part:
        classification_part = f"""LEFT JOIN (
        {classification_part}
    ) label_data_classification
    ON r.id = label_data_classification.record_id AND r.project_id = label_data_classification.project_id
        """

    if extraction_part:
        extraction_part = f"""
    INNER JOIN (
        {extraction_part}
    ) label_data_extraction
    ON r.id = label_data_extraction.record_id AND r.project_id = label_data_extraction.project_id
        """
    if not order_by_part:
        order_by_part = "1"
    return (
        base_query_manual
        + f"""
        
        SELECT 
            {select_part}
        FROM {__get_from_part(user_session_part)}
        {classification_part}
        {extraction_part}
        WHERE r.project_id = '{project_id}'
        ORDER BY {order_by_part}
        """
    )


def __get_case_information_extraction(project_id: str) -> List[Any]:
    sql = f"""
    SELECT *, 'CASE lt.id WHEN ''' || task_id || ''' THEN ltl.name END ' || Replace(col_name,'''','''''') AS case_task
    FROM (
        SELECT  '"' || attribute_name || '__' || task_name || '"' col_name,  *
        FROM (
            SELECT lt.name task_name, lt.task_type, lt.id task_id, att.name attribute_name, att.id::TEXT attribute_id
            FROM labeling_task lt
            INNER JOIN attribute att
                ON lt.attribute_id = att.id AND att.project_id = '{project_id}'
            WHERE lt.task_type = '{enums.LabelingTaskType.INFORMATION_EXTRACTION.value}'
                ) inner_sql
        ) case_full
    """
    return general.execute_all(sql)


def __get_classification_labels(
    project_id: str, columns_str_agg: str, label_case: str
) -> str:
    return f"""
    SELECT 
        record_id, 
        project_id,
        {columns_str_agg}
    FROM (
        SELECT 
            rla.record_id, 
            rla.project_id,
            rla.source_type, 
            vri.rla_id vri_rla_id,
            ROUND(rla.confidence::numeric,4) confidence,
            {label_case}
        FROM record_label_association rla
        LEFT JOIN valid_rla_ids vri
            ON rla.id = vri.rla_id
        WHERE rla.return_type = '{enums.InformationSourceReturnType.RETURN.value}' AND rla.project_id = '{project_id}' 
        AND rla.source_type IN ('{enums.LabelSource.MANUAL.value}','{enums.LabelSource.WEAK_SUPERVISION.value}')
        ) inner_sql
    GROUP BY record_id, project_id
    """

    # example columns_str_agg

    # string_agg("__Final Breakdown",', ') FILTER (WHERE source_type = 'MANUAL')  AS "__Final Breakdown__MANUAL",
    # string_agg("__Final Breakdown",', ') FILTER (WHERE source_type = 'WEAK_SUPERVISION')  AS "__Final Breakdown__WEAK_SUPERVISION",
    # string_agg("title__Sentiment",', ') FILTER (WHERE source_type = 'MANUAL')  AS "title__Sentiment__MANUAL",
    # string_agg("title__Sentiment",', ') FILTER (WHERE source_type = 'WEAK_SUPERVISION')  AS "title__Sentiment__WEAK_SUPERVISION"

    # example label_case

    # CASE labeling_task_label_id WHEN 'aaafdb18-13de-42f5-a5a3-f6eab7982078' THEN 'Buy' WHEN 'a5986ee7-9ad1-4f16-b194-8daab2653afb' THEN 'Dont' ELSE NULL END "__Final Breakdown",
    # CASE labeling_task_label_id WHEN 'cf613784-c91a-451f-84fb-1997a2f83edd' THEN 'Positive' WHEN 'c798e9d4-9e66-4295-a24a-ce81dc4d20be' THEN 'Negative' WHEN '20f6136a-837f-4806-9716-36da74a9c910' THEN 'Neutral' ELSE NULL END "title__Sentiment"


def __get_extraction_labels_BIO(
    project_id: str,
    token_limit: int,
    columns_arr_agg: str,
    columns_case_att_id: str,
    columns_case_BI: str,
    columns_case_labeling_task: str,
    session_sql: str,
) -> str:
    return f"""
    SELECT 
        record_id,
        project_id,
        {columns_arr_agg}
     FROM (
        SELECT 
            token_index.record_id,
            token_index.project_id,
            token_index.token_index,    
            token_index.attribute_id,    
            token_index.source_type,
            {columns_case_att_id}
        FROM (
            SELECT *
            FROM (SELECT '{enums.LabelSource.MANUAL.value}' source_type UNION SELECT '{enums.LabelSource.WEAK_SUPERVISION.value}') type_mult,
            (SELECT *
            FROM (
                SELECT ROW_NUMBER () OVER () -1 AS token_index
                FROM record_attribute_token_statistics
                LIMIT {token_limit} ) token_index,
            (
                SELECT r.id record_id, r.project_id,rats.attribute_id, MAX(rats.num_token) AS max_token
                FROM {__get_from_part(session_sql)}
                INNER JOIN record_attribute_token_statistics rats
                    ON r.id = rats.record_id
                WHERE r.project_id = '{project_id}'
                GROUP BY r.id, r.project_id, rats.attribute_id ) record_max_token
            WHERE token_index < max_token) base_token_index
    ) token_index
        LEFT JOIN (
            SELECT record_id,    
                    project_id,
                    source_type,
                    confidence ,
                    token_index,
                    is_beginning_token, 
                    attribute_id,
                    {columns_case_BI}
            FROM (
                SELECT 
                    rla.record_id,    
                    rla.project_id,
                    ROUND(rla.confidence::numeric,4) confidence,
                    rla.source_type,
                    rlat.token_index,
                    rlat.is_beginning_token,
                    lt.attribute_id,
                    {columns_case_labeling_task}
                FROM record_label_association rla
                LEFT JOIN valid_rla_ids vri
                    ON rla.id = vri.rla_id
                INNER JOIN record_label_association_token rlat
                    ON rla.id = rlat.record_label_association_id
                INNER JOIN labeling_task_label ltl
                    ON rla.labeling_task_label_id = ltl.id
                INNER JOIN labeling_task lt
                    ON lt.id = ltl.labeling_task_id
                WHERE rla.return_type = '{enums.InformationSourceReturnType.YIELD.value}' 
                    AND rla.project_id ='{project_id}' 
                    AND rla.source_type IN ('{enums.LabelSource.MANUAL.value}','{enums.LabelSource.WEAK_SUPERVISION.value}')
                    AND ((rla.source_type = '{enums.LabelSource.MANUAL.value}' AND vri.rla_id IS NOT NULL) OR rla.source_type ='{enums.LabelSource.WEAK_SUPERVISION.value}') 
                )inner_sql
                ) BIO_part
            ON token_index.token_index = BIO_part.token_index AND token_index.record_id = BIO_part.record_id AND token_index.project_id = BIO_part.project_id 
                AND token_index.attribute_id = BIO_part.attribute_id AND token_index.source_type = BIO_part.source_type
            )bio_full
    WHERE project_id ='{project_id}'
    GROUP BY record_id, project_id
    """
    #    ARRAY_AGG("content__Extract Data" ORDER BY record_id, token_index) FILTER (WHERE "content__Extract Data" IS NOT NULL AND source_type = 'MANUAL') AS "content__Extract Data__MANUAL",
    #     ARRAY_AGG("title__Something interessting" ORDER BY record_id, token_index) FILTER (WHERE "title__Something interessting" IS NOT NULL AND source_type = 'MANUAL') AS "title__Something interessting__MANUAL",
    #     ARRAY_AGG("content__Extract Data" ORDER BY record_id, token_index) FILTER (WHERE "content__Extract Data" IS NOT NULL AND source_type = 'WEAK_SUPERVISION') AS "content__Extract Data__WEAK_SUPERVISION",
    #     ARRAY_AGG("title__Something interessting" ORDER BY record_id, token_index) FILTER (WHERE "title__Something interessting" IS NOT NULL AND source_type = 'WEAK_SUPERVISION') AS "title__Something interessting__WEAK_SUPERVISION"

    # CASE WHEN BIO_part.token_index IS NULL AND token_index.attribute_id = '6d90df0c-8db1-43e5-a0d8-0e96f3ecffba' THEN 'O' ELSE "content__Extract Data" END AS "content__Extract Data",

    # CASE is_beginning_token WHEN TRUE THEN 'B-' WHEN FALSE THEN 'I-' END || "content__Extract Data" AS "content__Extract Data",
    # CASE is_beginning_token WHEN TRUE THEN 'B-' WHEN FALSE THEN 'I-' END || "title__Something interessting" AS "title__Something interessting"

    # CASE lt.id WHEN 'ffc8c403-23fa-4684-9470-e733168e8324' THEN ltl.name END "title__Something interessting",
    # CASE lt.id WHEN 'becc9f74-1549-4f8a-b295-52321787d416' THEN ltl.name END "content__Extract Data"


def __get_case_classification(project_id: str) -> List[Any]:
    sql = f"""
    SELECT *
    FROM (
        SELECT '"' || col_name || '"' col_name, MIN(attribute_id) attribute_id, MIN(task_name) task_name, MIN(task_type) task_type, 'CASE labeling_task_label_id ' || string_agg(case_part, ' ') || ' ELSE NULL END "' || col_name || '"' case_full
        FROM (
            SELECT  COALESCE(attribute_name,'') || '__' ||  COALESCE(task_name,'') col_name,  *, 'WHEN ''' || label_id || ''' THEN ''' || Replace(label_name,'''','''''')  || '''' AS case_part
            FROM (
                SELECT lt.name task_name, lt.task_type, ltl.name label_name, ltl.id::TEXT label_id, att.name attribute_name, att.id::TEXT attribute_id
                FROM labeling_task lt
                INNER JOIN labeling_task_label ltl
                    ON lt.id = ltl.labeling_task_id AND ltl.project_id = '{project_id}'
                LEFT JOIN attribute att
                    ON lt.attribute_id = att.id AND att.project_id = '{project_id}'
                WHERE lt.task_type = '{enums.LabelingTaskType.CLASSIFICATION.value}'
                    ) inner_sql
                ) mid_sql
        GROUP BY col_name 
        ) outer_sql
    """
    return general.execute_all(sql)


def __get_max_token(project_id: str, session_sql: Optional[str] = None) -> int:
    sql = f"""    
    SELECT MAX(rats.num_token) AS max_token
    FROM {__get_from_part(session_sql)}
    INNER JOIN record_attribute_token_statistics rats
        ON r.id = rats.record_id
    WHERE r.project_id = '{project_id}'
    """
    return general.execute_first(sql).max_token


def __get_from_part(session_sql: Optional[str] = None) -> str:
    if session_sql:
        return f"""
        ({session_sql}) us
        INNER JOIN record r
            ON us.record_id = r.id
        """
    return "record r"
