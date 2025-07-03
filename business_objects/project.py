from typing import List, Optional, Any, Dict, Union, Set
from sqlalchemy.sql import func, cast
from sqlalchemy.sql.functions import coalesce
from sqlalchemy import Integer
from . import general, attribute
from .. import enums
from ..session import session
from ..models import Project, Record, Attribute
from ..integration_objects.helper import (
    REFINERY_ATTRIBUTE_ACCESS_GROUPS,
    REFINERY_ATTRIBUTE_ACCESS_USERS,
)
from ..util import prevent_sql_injection

QUEUE_PROJECT_NAME = "@@HIDDEN_QUEUE_PROJECT@@"


def get(project_id: str) -> Project:
    return session.query(Project).filter(Project.id == project_id).first()


def get_with_labling_tasks_info_attributes(project_id: str) -> Project:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    labeling_task_query = __build_sql_labeling_tasks_by_project(project_id)
    information_sources_query = __build_sql_information_sources_by_project(project_id)
    attributes_query = __build_sql_attributes_by_project(project_id)
    data_slice_query = __build_sql_data_slices_by_project(project_id)

    return {
        "project_id": project_id,
        "labeling_tasks": general.execute_first(labeling_task_query)[0],
        "information_sources": general.execute_first(information_sources_query)[0],
        "attributes": general.execute_first(attributes_query)[0],
        "data_slices": general.execute_first(data_slice_query)[0],
    }


def __build_sql_labeling_tasks_by_project(project_id: str) -> str:
    return f"""
    SELECT
        json_agg(json_build_object(
            'id', labeling_task.id,
            'name', labeling_task.name,
            
            'attribute', 
            CASE
                WHEN attribute.id IS NULL THEN NULL
                ELSE json_build_object(
                    'relative_position', attribute.relative_position
                    )
            END
        )) AS labeling_task
    FROM
        project
    LEFT JOIN
        labeling_task
            ON project.id = labeling_task.project_id
    LEFT JOIN
        attribute
            ON labeling_task.attribute_id = attribute.id
    WHERE
        project.id = '{project_id}'::UUID;
            """


def __build_sql_information_sources_by_project(project_id: str) -> str:
    return f"""
    SELECT
         json_agg(json_build_object(
            'id', information_source.id,
            'name', information_source.name
        )) AS information_sources
    FROM
        project
    LEFT JOIN
        information_source
            ON project.id = information_source.project_id
    WHERE
        project.id = '{project_id}'::UUID;
            """


def __build_sql_attributes_by_project(project_id: str) -> str:
    return f"""
    SELECT
         json_agg(json_build_object(
            'id', attribute.id,
            'name', attribute.name,
            'state', attribute.state
        )) AS attributes
    FROM
        project
    LEFT JOIN
        attribute
            ON project.id = attribute.project_id
    WHERE
        project.id = '{project_id}'::UUID;
            """


def __build_sql_data_slices_by_project(project_id: str) -> str:
    return f"""
    SELECT
         json_agg(json_build_object(
            'id', data_slice.id,
            'name', data_slice.name,
            'slice_type', data_slice.slice_type,
            'created_at', data_slice.created_at
        )) AS data_slices
    FROM
        project
    LEFT JOIN
        data_slice
            ON project.id = data_slice.project_id
    WHERE
        project.id = '{project_id}'::UUID; """


def get_dropdown_list_project_list(
    org_id: str, project_id: Optional[str] = None
) -> List[Dict[str, str]]:
    org_id = prevent_sql_injection(org_id, isinstance(org_id, str))
    prj_filter = ""
    if project_id:
        project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
        prj_filter = f"AND p.id = '{project_id}'"
    query = f"""
    SELECT array_agg(jsonb_build_object('value', p.id,'name',p.NAME))
    FROM public.project p
    WHERE p.organization_id = '{org_id}' AND p.status != '{enums.ProjectStatus.HIDDEN.value}' {prj_filter}
    """
    values = general.execute_first(query)

    if values and values[0]:
        return values[0]
    return []


def get_org_id(project_id: str) -> str:
    if p := get(project_id):
        return str(p.organization_id)
    raise ValueError(f"Project with id {project_id} not found")


def get_with_organization_id(organization_id: str, project_id: str) -> Project:
    return (
        session.query(Project)
        .filter(
            Project.organization_id == organization_id,
            Project.id == project_id,
        )
        .first()
    )


def get_all(organization_id: str) -> List[Project]:
    return (
        session.query(Project).filter(Project.organization_id == organization_id).all()
    )


def get_all_with_access_management(org_id: str) -> List[Dict[str, Any]]:
    org_id_safe = prevent_sql_injection(org_id, isinstance(org_id, str))

    hidden_status = enums.ProjectStatus.HIDDEN.value
    permission_data_type = enums.DataTypes.PERMISSION.value
    automatically_created_state = enums.AttributeState.AUTOMATICALLY_CREATED.value
    access_groups_attr = REFINERY_ATTRIBUTE_ACCESS_GROUPS
    access_users_attr = REFINERY_ATTRIBUTE_ACCESS_USERS

    query = f"""
    SELECT DISTINCT
            p.*,
            COALESCE((ci.config -> 'extract_kwargs' ->> 'sync_sharepoint_permissions')::BOOLEAN,FALSE) AS is_sharepoint_sync_active
        FROM
            public.project p
        JOIN
            public.attribute a ON p.id = a.project_id
        LEFT JOIN
            cognition.integration ci ON p.id = ci.project_id
        WHERE
            p.organization_id = '{org_id_safe}'
            AND p.status != '{hidden_status}'
            AND a.name IN ('{access_groups_attr}', '{access_users_attr}')
            AND a.user_created = FALSE
            AND a.data_type = '{permission_data_type}'
            AND a.state = '{automatically_created_state}';
    """

    values = general.execute_all(query)
    return values


def check_access_management_active(project_id: str) -> bool:
    return (
        session.query(Project)
        .join(Attribute, Project.id == Attribute.project_id)
        .filter(
            Project.id == project_id,
            Attribute.name.in_(
                [REFINERY_ATTRIBUTE_ACCESS_GROUPS, REFINERY_ATTRIBUTE_ACCESS_USERS]
            ),
            Attribute.user_created == False,
            Attribute.data_type == enums.DataTypes.PERMISSION.value,
            Attribute.state == enums.AttributeState.AUTOMATICALLY_CREATED.value,
        )
        .count()
        > 0
    )


def get_all_by_user_organization_id(organization_id: str) -> List[Project]:
    projects = (
        session.query(Project).filter(Project.organization_id == organization_id).all()
    )
    return projects


def get_all_all() -> List[Project]:
    return session.query(Project).all()


def get_all_all_ids() -> Set[str]:
    return {str(p.id) for p in session.query(Project.id).all()}


def get_blank_tokenizer_from_project(project_id: str) -> str:
    project_item = get(project_id)
    return (
        project_item.tokenizer[:2]
        if not project_item.tokenizer_blank
        else project_item.tokenizer_blank
    )


def get_max_running_id(project_id: str) -> int:
    running_id_like_name = attribute.get_running_id_name(project_id)
    if not running_id_like_name:
        raise ValueError("Can't find running_id column")

    max_running_id = (
        session.query(
            func.max(
                coalesce(cast(Record.data.op("->>")(running_id_like_name), Integer), -1)
            )
        )
        .filter(
            Record.project_id == project_id,
        )
        .scalar()
    )
    return max_running_id or -1


def get_general_project_stats(
    project_id: str,
    labeling_task_id: Optional[str] = None,
    slice_id: Optional[str] = None,
) -> List[Dict[str, Union[str, float]]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    labeling_task_id = prevent_sql_injection(
        labeling_task_id, isinstance(labeling_task_id, str)
    )
    slice_id = prevent_sql_injection(slice_id, isinstance(slice_id, str))
    values = general.execute_first(
        __build_sql_project_stats(project_id, labeling_task_id, slice_id)
    )
    if values:
        return values[0]


def get_label_distribution(
    project_id: str,
    labeling_task_id: Optional[str] = None,
    slice_id: Optional[str] = None,
) -> List[Dict[str, Union[str, float]]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    labeling_task_id = prevent_sql_injection(
        labeling_task_id, isinstance(labeling_task_id, str)
    )
    slice_id = prevent_sql_injection(slice_id, isinstance(slice_id, str))
    values = general.execute_first(
        __build_sql_label_distribution(project_id, labeling_task_id, slice_id)
    )
    if values:
        return values[0]


def get_or_create_queue_project(
    org_id: str, user_id: str, with_commit: bool = False
) -> Project:
    ## user_id is a "last used by" indicator

    prj = (
        session.query(Project)
        .filter(
            Project.organization_id == org_id,
            Project.name == QUEUE_PROJECT_NAME,
        )
        .first()
    )

    if prj:
        if str(prj.created_by) != user_id:
            prj.created_by = user_id
            general.flush_or_commit(with_commit)
        return prj

    prj = create(
        org_id,
        QUEUE_PROJECT_NAME,
        "Queue project for org specific queue tasks",
        user_id,
        with_commit=False,
    )

    prj.status = enums.ProjectStatus.HIDDEN.value
    general.flush_or_commit(with_commit)
    return prj


def create(
    organization_id: str,
    name: str,
    description: str,
    created_by: str,
    created_at: Optional[str] = None,
    tokenizer: Optional[str] = None,
    tokenizer_blank: Optional[str] = None,
    with_commit: bool = False,
    status: enums.ProjectStatus = enums.ProjectStatus.INIT_UPLOAD,
) -> Project:
    project: Project = Project(
        name=name,
        description=description,
        organization_id=organization_id,
        created_by=created_by,
        created_at=created_at,
        status=status.value,
        tokenizer=tokenizer,
        tokenizer_blank=tokenizer_blank,
    )
    general.add(project, with_commit)
    return project


def delete(project_id: str, with_commit: bool = False) -> None:
    import time

    start_time = time.time()
    session.query(Project).filter(
        Project.id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)
    print("finished delete in", (time.time() - start_time))


def delete_by_id(project_id: str, with_commit: bool = False) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    base_query = f"""
    DELETE FROM @@TBL@@
    WHERE @@COL@@ = '{project_id}' """
    # ordered by relation depth to ensure cleanup from bottom up
    table_query = """
    WITH RECURSIVE  relations AS (
        SELECT classid, objid, objsubid, conrelid, 0 deep
        FROM pg_depend d
        INNER JOIN pg_constraint c 
            ON c.oid = objid
        WHERE refobjid = 'project'::regclass AND deptype = 'n'
    UNION ALL 
        SELECT  d.classid, d.objid, d.objsubid, c.conrelid, deep +1 deep
        FROM pg_depend d
        INNER JOIN pg_constraint c on c.oid = objid
        INNER JOIN relations on d.refobjid = relations.conrelid and d.deptype = 'n'
        WHERE relations.deep < 20
        )        
        
    SELECT rel.*,col.column_name
    FROM (
        SELECT
            conrelid::REGCLASS::TEXT tbl,
            MAX(deep) deep
        FROM relations
        GROUP BY conrelid::REGCLASS) rel
    INNER JOIN (
        SELECT t.table_name,col.column_name
        FROM information_schema.tables t
        INNER JOIN information_schema.columns col
            ON col.table_name = t.table_name AND col.table_schema = t.table_schema
        WHERE col.column_name = 'project_id'
        AND t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    ) col
        ON tbl = col.table_name
    UNION ALL 
    SELECT 'project', -1, 'id'
    ORDER BY 2 DESC
    """
    for row in general.execute_all(table_query):
        general.execute(
            base_query.replace("@@TBL@@", row[0]).replace("@@COL@@", row[2])
        )
        general.flush_or_commit(with_commit)


def update(
    project_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    status: Optional[str] = None,
    tokenizer: Optional[str] = None,
    with_commit: bool = False,
) -> Project:
    project: Project = session.query(Project).get(project_id)

    if name:
        project.name = name

    if description is not None:
        project.description = description

    if status is not None:
        project.status = status

    if tokenizer is not None:
        spacy_language = tokenizer[:2]
        project.tokenizer = tokenizer
        project.tokenizer_blank = spacy_language
    general.flush_or_commit(with_commit)
    return project


def __build_sql_label_distribution(
    project_id: str,
    labeling_task_id: Optional[str] = None,
    slice_id: Optional[str] = None,
) -> str:
    labeling_task_filter = ""
    if labeling_task_id:
        labeling_task_id = f"AND ltl.labeling_task_id = '{labeling_task_id}'"
        labeling_task_filter = f"""
        INNER JOIN labeling_task_label ltl
            ON rla.labeling_task_label_id = ltl.id AND rla.project_id = ltl.project_id {labeling_task_id} """
    else:
        labeling_task_id = ""

    slice_filter = ""
    if slice_id:
        slice_filter = f"""
        INNER JOIN data_slice_record_association dsra
            ON rla.record_id = dsra.record_id AND rla.project_id = dsra.project_id AND dsra.data_slice_id = '{slice_id}' """

    return f"""
    WITH labels_count AS (
        SELECT labeling_task_label_id label_id,source_type, COUNT(*) count_absolute
        FROM (
            SELECT rla.record_id,rla.source_type,rla.labeling_task_label_id
            FROM record_label_association rla {labeling_task_filter} {slice_filter}
            WHERE rla.project_id = '{project_id}' AND (rla.is_valid_manual_label OR rla.is_valid_manual_label IS NULL)
                AND rla.source_type IN ('{enums.LabelSource.MANUAL.value}','{enums.LabelSource.WEAK_SUPERVISION.value}')
            GROUP BY rla.record_id,rla.source_type,rla.labeling_task_label_id ) x
        GROUP BY labeling_task_label_id,source_type
    ),relevant_sources AS (
        SELECT '{enums.LabelSource.MANUAL.value}' source_type UNION ALL 
        SELECT '{enums.LabelSource.WEAK_SUPERVISION.value}')

    SELECT array_agg(row_to_json(x))
    FROM (
        SELECT l.*,COALESCE(x.count_absolute,0) count_absolute,COALESCE(x.count_relative,0) count_relative
        FROM (
            SELECT ltl.id,ltl.name, rs.source_type
            FROM labeling_task_label ltl, relevant_sources rs
            WHERE ltl.project_id = '{project_id}' {labeling_task_id}) l
        LEFT JOIN (
            SELECT lc.label_id,lc.source_type,lc.count_absolute,CASE WHEN s.sum_sum = 0 THEN 0 ELSE round(lc.count_absolute::numeric/s.sum_sum,4) END count_relative
            FROM labels_count lc
            INNER JOIN (
                SELECT source_type, SUM(count_absolute) sum_sum
                FROM labels_count lc
                GROUP BY source_type)s
                ON lc.source_type = s.source_type
        ) x
            ON l.id = x.label_id AND l.source_type = x.source_type)x """


def __build_sql_project_stats(
    project_id: str,
    labeling_task_id: Optional[str] = None,
    slice_id: Optional[str] = None,
) -> str:
    labeling_task_filter = ""
    labeling_task_filter_is = ""
    if labeling_task_id:
        labeling_task_filter_is = f"AND _is.labeling_task_id = '{labeling_task_id}'"
        labeling_task_filter = f"""
        INNER JOIN labeling_task_label ltl
            ON rla.labeling_task_label_id = ltl.id AND rla.project_id = ltl.project_id AND ltl.labeling_task_id = '{labeling_task_id}' """
    slice_filter = ""
    slice_filter_records = ""
    if slice_id:
        slice_filter = f"""
        INNER JOIN data_slice_record_association dsra
            ON rla.record_id = dsra.record_id AND rla.project_id = dsra.project_id AND dsra.data_slice_id = '{slice_id}' """
        slice_filter_records = f"""
        INNER JOIN data_slice_record_association dsra
            ON r.id = dsra.record_id AND dsra.project_id = r.project_id AND dsra.data_slice_id = '{slice_id}' """

    return f"""
    WITH relevant_sources AS (
    SELECT '{enums.LabelSource.MANUAL.value}' source_type UNION ALL 
    SELECT '{enums.LabelSource.WEAK_SUPERVISION.value}')

    SELECT array_agg(row_to_json(x))
    FROM (
    SELECT source_type,counts.c absolut_labeled,max_counts.max_records records_in_slice , round(counts.c::numeric/max_counts.max_records,4) percent
    FROM (
        SELECT rs.source_type, COUNT(x.*) c
        FROM relevant_sources rs
        LEFT JOIN (
            SELECT rla.record_id, rla.source_type
            FROM record_label_association rla {labeling_task_filter} {slice_filter}
            WHERE rla.project_id = '{project_id}' AND (rla.source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}' 
                OR (rla.source_type = '{enums.LabelSource.MANUAL.value}' AND rla.is_valid_manual_label)) 
            GROUP BY rla.record_id,rla.source_type )x
            ON rs.source_type = x.source_type
        GROUP BY rs.source_type ) counts,
    (
        SELECT COUNT(*) max_records
        FROM record r {slice_filter_records}
        WHERE r.project_id = '{project_id}'
        AND r.category = '{enums.RecordCategory.SCALE.value}'
    ) max_counts
    UNION ALL 
    SELECT '{enums.LabelSource.INFORMATION_SOURCE.value}', count_in_slice, count_absolute, -1
    FROM (	
        SELECT COUNT(*) count_absolute
        FROM information_source _is
        WHERE _is.project_id = '{project_id}' {labeling_task_filter_is}
    )x, (
        SELECT COUNT(*) count_in_slice
        FROM (
            SELECT rla.source_id
            FROM record_label_association rla {labeling_task_filter} {slice_filter}
            WHERE rla.source_type = '{enums.LabelSource.INFORMATION_SOURCE.value}'
            GROUP BY rla.source_id
        )y
    )y)x    
    """


def get_project_size(project_id: str) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    return general.execute_all(__get_project_size_sql(project_id))


def __get_project_size_sql(project_id: str) -> str:
    return f"""
        SELECT order_, table_, description, prj_size_bytes, pg_size_pretty(prj_size_bytes) prj_size_readable
        FROM (
            SELECT order_, table_, description, COALESCE(prj_size_bytes,0) prj_size_bytes
            FROM (
                SELECT 7 order_, 'embeddings' table_, NULL description, sum(pg_column_size(e.*)) prj_size_bytes
                FROM embedding e
                WHERE project_id = '{project_id}'
                UNION ALL
                SELECT 8 order_, 'embedding tensors' table_, 'will be recalculated on import' description, sum(pg_column_size(et.*)) prj_size_bytes
                FROM embedding_tensor et
                WHERE project_id = '{project_id}'
                UNION ALL 
                SELECT 5 order_, 'information sources' table_, NULL description, sum(pg_column_size(in_s.*) + pg_column_size(iss.*)) prj_size_bytes
                FROM information_source in_s
                INNER JOIN information_source_statistics iss
                    ON in_s.id = iss.source_id AND in_s.project_id = iss.project_id
                WHERE in_s.project_id = '{project_id}'  
                UNION ALL 
                SELECT 6 order_, 'information sources payloads' table_, 'not needed to start a new run' description, sum(pg_column_size(isp.*)) prj_size_bytes
                FROM (
                    SELECT isp.id, isp.source_id,isp.source_code,isp.state, isp.created_at, isp.finished_at, isp.iteration,isp.logs,isp.created_by, isp.project_id
                    FROM information_source_payload isp )isp
                WHERE project_id = '{project_id}'
                UNION ALL 
                SELECT 9 order_, 'knowledge bases' table_, NULL description, sum(pg_column_size(kb.*)+pg_column_size(kt.*)) prj_size_bytes
                FROM knowledge_base kb
                INNER JOIN knowledge_term kt
                    ON kb.id = kt.knowledge_base_id AND kb.project_id = kt.project_id
                WHERE kb.project_id = '{project_id}'
                UNION ALL 
                SELECT 0 order_, 'basic project data' table_, 'includes project, attributes, labeling tasks, labels & data slices' description, 
                                    SUM(prj_size) prj_size_bytes
                FROM (
                    SELECT 'project' tbl_name, COALESCE(sum(pg_column_size(p.*)),0) prj_size
                    FROM project p
                    WHERE p.id = '{project_id}'
                    UNION ALL
                    SELECT 'data_slice' tbl_name,COALESCE(sum(pg_column_size(ds.*)),0) prj_size
                    FROM data_slice ds
                    WHERE ds.project_id = '{project_id}'
                    UNION ALL
                    SELECT 'data_slice_record_association' tbl_name,COALESCE(sum(pg_column_size(dsra.*)),0) prj_size
                    FROM data_slice_record_association dsra
                    WHERE dsra.project_id = '{project_id}'
                    UNION ALL
                    SELECT 'attribute' tbl_name,COALESCE(sum(pg_column_size(a.*)),0) prj_size
                    FROM attribute a
                    WHERE a.project_id = '{project_id}'
                    UNION ALL
                    SELECT 'labeling_task' tbl_name,COALESCE(sum(pg_column_size(lt.*)),0) prj_size
                    FROM labeling_task lt
                    WHERE lt.project_id = '{project_id}'
                    UNION ALL
                    SELECT 'labeling_task_label' tbl_name,COALESCE(sum(pg_column_size(ltl.*)),0) prj_size
                    FROM labeling_task_label ltl
                    WHERE ltl.project_id = '{project_id}' ) helper
                UNION ALL 
                SELECT 1 order_, 'records' table_, NULL description, sum(pg_column_size(record.*)) prj_size_bytes
                FROM record
                WHERE project_id = '{project_id}'
                UNION ALL 
                SELECT 3 order_, 'record attribute token statistics' table_, 'will be recalculated on import' description, sum(pg_column_size(rats.*)) prj_size_bytes
                FROM record_attribute_token_statistics rats
                WHERE project_id = '{project_id}'
                UNION ALL 
                SELECT 2 order_, 'record label associations' table_, NULL description, sum(pg_column_size(rla.*) ) + COALESCE(sum(pg_column_size(rlat.*)),0) prj_size_bytes
                FROM record_label_association rla
                LEFT JOIN record_label_association_token rlat
                    ON rla.id = rlat.record_label_association_id AND rla.project_id = rlat.project_id
                WHERE rla.project_id = '{project_id}'
                UNION ALL
                SELECT 10 order_, 'comment data' table_, NULL description, sum(pg_column_size(cd.*)) prj_size_bytes
                FROM comment_data cd
                WHERE cd.project_id = '{project_id}'
            )i
        ) x
        ORDER BY order_
    """


def get_project_by_project_id_sql(project_id: str) -> Dict[str, Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))

    query = f"""
    SELECT row_to_json(y)
    FROM (
        SELECT 
            id,
            NAME,
            description,
            NULL AS project_type,
            tokenizer,
            CASE 
                WHEN status = 'IN_DELETION' THEN -1
                ELSE r_count
            END num_data_scale_uploaded
        FROM project p,
        (
            SELECT COUNT(*) r_count FROM record WHERE project_id = '{project_id}' 
        )x
        WHERE p.id = '{project_id}' )y
    """
    value = general.execute_first(query)
    if value:
        return value[0]
    else:
        return None
