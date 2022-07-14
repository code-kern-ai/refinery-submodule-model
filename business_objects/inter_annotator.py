from typing import List, Any

from . import general
from .. import enums


def get_current_inter_annotator_classification_users(
    project_id: str, labeling_task_id: str, slice_id: str
) -> List[Any]:
    query = __get_base_query_classification_user_to_labling_task(
        project_id, labeling_task_id, slice_id
    )
    query += """
    SELECT user_id, COUNT(*) distinct_records
    FROM (
        SELECT user_id, record_id
        FROM user_labels
        GROUP BY user_id, record_id )x
    GROUP BY user_id 
    ORDER BY user_id """

    return general.execute_all(query)


def get_all_inter_annotator_classification_users(
    project_id: str, labeling_task_id: str, slice_id: str
) -> List[Any]:
    query = __get_base_query_classification_user_to_labling_task(
        project_id, labeling_task_id, slice_id
    )
    query += f"""
    SELECT COALESCE(org_user.id,x.user_id) user_id, COALESCE(x.distinct_records,0) distinct_records
    FROM (
        SELECT u.id::TEXT
        FROM project p 
        INNER JOIN public.user u
            ON p.organization_id = u.organization_id
        WHERE p.id = '{project_id}' )org_user
    FULL OUTER JOIN (
        SELECT user_id, COUNT(*) distinct_records
        FROM (
            SELECT user_id, record_id
            FROM user_labels
            GROUP BY user_id, record_id )x
        GROUP BY user_id 
        ORDER BY user_id )x
        ON org_user.id = x.user_id
    """

    return general.execute_all(query)


def get_classification_user_by_user_label_count(
    project_id: str, labeling_task_id: str, slice_id: str
) -> List[Any]:
    query = __get_base_query_classification_user_to_labling_task(
        project_id, labeling_task_id, slice_id
    )
    query += """
    SELECT user_lookup, round(count_same/full_count::NUMERIC,4) percent
    FROM (
        SELECT user_id || '@' || other_user user_lookup, SUM(same_answer) count_same, COUNT(*) full_count
        FROM (
            SELECT a.*,b.user_id other_user, CASE WHEN a.labeling_task_label_id = b.labeling_task_label_id THEN 1 ELSE 0 END same_answer
            FROM user_labels a
            INNER JOIN user_labels b
                ON a.record_id = b.record_id AND a.user_id != b.user_id )x
        GROUP BY user_id,other_user
        ORDER BY user_id,other_user )x """

    return general.execute_all(query)


def get_extraction_user_max_lookup(
    project_id: str, labeling_task_id: str, slice_id: str
) -> List[Any]:

    query = f"""
    WITH  relevant_rlas AS(
        SELECT rla.id, rla.record_id, CASE WHEN is_gold_star IS NOT NULL THEN '{enums.InterAnnotatorConstants.ID_GOLD_USER.value}' ELSE rla.created_by::TEXT END user_id
        FROM record_label_association rla
        {__get_slice_add(slice_id)}
        INNER JOIN labeling_task_label ltl
        ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id  
        WHERE rla.project_id = '{project_id}' AND ltl.labeling_task_id = '{labeling_task_id}'
            AND rla.source_type = '{enums.LabelSource.MANUAL.value}' 
    ),users AS (
        SELECT user_id
        FROM relevant_rlas
        GROUP BY user_id
    )

    SELECT user_id || '@' || other_user user_lookup,COUNT(rla_id) possible_matches
    FROM (
        SELECT u.user_id, u.other_user,rlaX.*,rla.id rla_id
        FROM (
            SELECT u1.user_id, u2.user_id other_user
            FROM users u1
            INNER JOIN users u2
                ON u1.user_id != u2.user_id )u
        LEFT JOIN (
            SELECT record_id, array_agg(DISTINCT user_id) user_ids
            FROM relevant_rlas
            GROUP BY record_id
            ) rlaX
            ON u.user_id = ANY(user_ids) AND u.other_user = ANY(user_ids)
        LEFT JOIN relevant_rlas rla
            ON rla.record_id = rlaX.record_id AND (rla.user_id = u.user_id OR rla.user_id = u.other_user)
        ) x
    GROUP BY user_id, other_user """

    return general.execute_all(query)


def get_inter_annotator_extraction_users(
    project_id: str,
    labeling_task_id: str,
    slice_id: str,
    all_user: bool,
) -> List[Any]:
    query = __get_base_query_extraction_user_to_labling_task(
        project_id, labeling_task_id, slice_id
    )
    if all_user:
        query += f"""
    SELECT COALESCE(org_user.id,x.user_id) user_id, COALESCE(x.distinct_records,0) distinct_records
    FROM (
        SELECT u.id::TEXT
        FROM project p 
        INNER JOIN public.user u
            ON p.organization_id = u.organization_id
        WHERE p.id = '{project_id}' )org_user
    FULL OUTER JOIN (
        SELECT user_id, COUNT(*) distinct_records
        FROM (
            SELECT user_id, record_id
            FROM user_labels
            GROUP BY user_id, record_id )x
        GROUP BY user_id 
        ORDER BY user_id )x
        ON org_user.id = x.user_id
    """
    else:
        query += """
    SELECT user_id, COUNT(*) distinct_records
    FROM (
        SELECT user_id, record_id
        FROM user_labels
        GROUP BY user_id, record_id )x
    GROUP BY user_id 
    ORDER BY user_id """

    return general.execute_all(query)


def get_extraction_user_by_user_label_count(
    project_id: str, labeling_task_id: str, slice_id: str
) -> List[Any]:
    query = __get_base_query_extraction_user_to_labling_task(
        project_id, labeling_task_id, slice_id
    )
    # count is multiplied by 2 since the matching are counted since every rla is concidered a possiblity a match means two are "found"
    query += """
    SELECT user_id || '@' || other_user user_lookup, COUNT(*)*2 count_same
    FROM (
        SELECT u1.*,u2.user_id other_user
        FROM user_labels u1
        INNER JOIN user_labels u2
            ON u1.record_id = u2.record_id AND u1.user_id != u2.user_id AND u1.t_index = u2.t_index )x
        GROUP BY user_id,other_user """

    return general.execute_all(query)


def check_inter_annotator_classification_records_only_used_once(
    project_id: str, labeling_task_id: str, slice_id: str
) -> Any:
    query = __get_base_query_classification_user_to_labling_task(
        project_id, labeling_task_id, slice_id
    )
    query += """
    SELECT SUM(c) sum,COUNT(*) count
    FROM user_labels"""
    return general.execute_first(query)


def __get_base_query_extraction_user_to_labling_task(
    project_id: str, labeling_task_id: str, slice_id: str
) -> str:
    return f"""
    WITH user_labels AS (
	SELECT 
		rla.record_id,
		ltl.labeling_task_id,
		CASE WHEN is_gold_star IS NOT NULL THEN '{enums.InterAnnotatorConstants.ID_GOLD_USER.value}' ELSE COALESCE(rla.created_by::TEXT,'{enums.InterAnnotatorConstants.ID_NULL_USER.value}') END user_id,
		rla.id rla_id,
		COUNT(*) c,
		array_agg(rlat.token_index || '-' || rla.labeling_task_label_id::TEXT ORDER BY rlat.token_index) t_index
	FROM record_label_association rla
    {__get_slice_add(slice_id)}
	INNER JOIN labeling_task_label ltl
	  ON rla.labeling_task_label_id = ltl.id AND ltl.project_id = rla.project_id  
	INNER JOIN record_label_association_token rlat
	  ON rla.id = rlat.record_label_association_id
	WHERE rla.project_id = '{project_id}' AND ltl.labeling_task_id = '{labeling_task_id}'
		AND rla.source_type = '{enums.LabelSource.MANUAL.value}'
	GROUP BY rla.record_id,ltl.labeling_task_id,user_id,rla.id)
    """


def __get_slice_add(slice_id: str) -> str:
    slice_add: str = ""
    if slice_id:
        slice_add = f"""INNER JOIN data_slice_record_association dsra
         	ON rla.record_id = dsra.record_id AND rla.project_id = dsra.project_id AND dsra.data_slice_id = '{slice_id}'"""
    return slice_add


def __get_base_query_classification_user_to_labling_task(
    project_id: str, labeling_task_id: str, slice_id: str
) -> str:
    return f"""
    WITH user_labels AS (
        SELECT rla.record_id, rla.labeling_task_label_id, CASE WHEN rla.is_gold_star IS NOT NULL THEN '{enums.InterAnnotatorConstants.ID_GOLD_USER.value}' ELSE COALESCE(rla.created_by::TEXT,'{enums.InterAnnotatorConstants.ID_NULL_USER.value}') END user_id,COUNT(*) c
        FROM record_label_association rla
        INNER JOIN labeling_task_label ltl
            ON rla.labeling_task_label_id = ltl.id AND rla.project_id = ltl.project_id
        INNER JOIN labeling_task lt
            ON ltl.labeling_task_id = lt.id AND ltl.project_id = lt.project_id
        {__get_slice_add(slice_id)}
        WHERE rla.project_id = '{project_id}' AND lt.id = '{labeling_task_id}'
      	    AND rla.source_type = '{enums.LabelSource.MANUAL.value}'
	    GROUP BY rla.record_id, rla.labeling_task_label_id, user_id)   
    """
