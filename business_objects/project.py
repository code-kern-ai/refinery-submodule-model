from typing import List, Optional, Any, Dict, Union
from sqlalchemy.sql import func

from . import general

from .. import enums
from ..session import session
from ..models import (
    DataSliceRecordAssociation,
    LabelingTask,
    LabelingTaskLabel,
    Project,
    RecordLabelAssociation,
)


def get(project_id: str) -> Project:
    return session.query(Project).filter(Project.id == project_id).first()


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


def get_blank_tokenizer_from_project(project_id: str) -> str:
    project_item = get(project_id)
    return (
        project_item.tokenizer[:2]
        if not project_item.tokenizer_blank
        else project_item.tokenizer_blank
    )


def get_general_project_stats(
    project_id: str,
    labeling_task_id: Optional[str] = None,
    slice_id: Optional[str] = None,
) -> List[Dict[str, Union[str, float]]]:
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
    values = general.execute_first(
        __build_sql_label_distribution(project_id, labeling_task_id, slice_id)
    )
    if values:
        return values[0]


def get_confidence_distribution(
    project_id: str,
    labeling_task_id: str,
    data_slice_id: Optional[str] = None,
    num_samples: Optional[int] = None,
) -> List[float]:
    query_filter = (
        session.query(RecordLabelAssociation.confidence)
        .join(
            LabelingTaskLabel,
            (RecordLabelAssociation.labeling_task_label_id == LabelingTaskLabel.id)
            & (LabelingTaskLabel.project_id == RecordLabelAssociation.project_id),
        )
        .join(
            LabelingTask,
            (LabelingTask.id == LabelingTaskLabel.labeling_task_id)
            & (LabelingTask.project_id == LabelingTaskLabel.project_id),
        )
        .filter(
            RecordLabelAssociation.project_id == project_id,
            LabelingTask.id == labeling_task_id,
            RecordLabelAssociation.source_type
            == enums.LabelSource.WEAK_SUPERVISION.value,
            RecordLabelAssociation.project_id == project_id,
        )
    )

    if data_slice_id is not None:
        query_filter = query_filter.join(
            DataSliceRecordAssociation,
            (DataSliceRecordAssociation.record_id == RecordLabelAssociation.record_id)
            & (
                DataSliceRecordAssociation.project_id
                == RecordLabelAssociation.project_id
            ),
        ).filter(
            DataSliceRecordAssociation.data_slice_id == data_slice_id,
        )

    if num_samples is not None:
        query_filter = query_filter.order_by(func.random()).limit(num_samples)
        general.set_seed(0)
        confidence_scores = [confidence for confidence, in (query_filter.all())]
        confidence_scores = sorted(confidence_scores)
    else:
        query_filter = query_filter.order_by(RecordLabelAssociation.confidence.asc())
        confidence_scores = [confidence for confidence, in (query_filter.all())]

    return confidence_scores


def get_confusion_matrix(
    project_id: str,
    labeling_task_id: str,
    for_classification: bool,
    slice_id: Optional[str] = None,
) -> List[Dict[str, Union[str, float]]]:
    if for_classification:
        values = general.execute_first(
            __build_sql_confusion_matrix_classification(
                project_id, labeling_task_id, slice_id
            )
        )
    else:
        values = general.execute_first(
            __build_sql_confusion_matrix_extraction(
                project_id, labeling_task_id, slice_id
            )
        )
    if values:
        return values[0]


def get_zero_shot_project_config(project_id: str, payload_id: str) -> Any:
    query = f"""
    SELECT base.*, a.name attribute_name
    FROM (
        SELECT 
            isp.source_id,
            isp.created_by,
            isp.source_code::JSON ->>'config' config,
            (isp.source_code::JSON ->>'min_confidence')::FLOAT min_confidence,
            (isp.source_code::JSON ->>'run_individually')::BOOLEAN run_individually,
            COALESCE(lt.attribute_id,(isp.source_code::JSON ->>'attribute_id')::UUID) attribute_id,
            ltl.label_names,
            ltl.label_ids
        FROM information_source_payload isp
        INNER JOIN information_source _is
                ON isp.project_id = _is.project_id AND isp.source_id = _is.id
        INNER JOIN labeling_task lt
            ON _is.labeling_task_id = lt.id
        INNER JOIN (
            SELECT ltl.labeling_task_id,array_agg(ltl.name ORDER BY ltl.id) label_names,array_agg(ltl.id ORDER BY ltl.id) label_ids
                FROM labeling_task_label ltl
                INNER JOIN (
                    SELECT _is.labeling_task_id, TRANSLATE((isp.source_code::JSON ->>'excluded_labels'), '[]','{{}}')::UUID[] excluded_labels
                    FROM information_source _is
                    INNER JOIN information_source_payload isp
                        ON _is.project_id = isp.project_id AND _is.id =isp.source_id
                    WHERE isp.id = '{payload_id}' AND isp.project_id = '{project_id}'
                ) isp
                    ON ltl.labeling_task_id = isp.labeling_task_id
                WHERE ltl.project_id = '{project_id}'
                    AND  NOT (ltl.id = ANY (isp.excluded_labels))
                GROUP BY ltl.labeling_task_id
        ) ltl
            ON _is.labeling_task_id = ltl.labeling_task_id
        WHERE isp.id = '{payload_id}' AND isp.project_id = '{project_id}' )base
    INNER JOIN attribute a
        ON a.id = base.attribute_id AND a.project_id = '{project_id}'
    """
    return general.execute_first(query)


def create(
    organization_id: str,
    name: str,
    description: str,
    created_by: str,
    created_at: Optional[str] = None,
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


def __build_sql_confusion_matrix_extraction(
    project_id: str,
    labeling_task_id: str,
    slice_id: Optional[str] = None,
) -> str:
    OUTSIDE_MARKER = "@@OUTSIDE@@"
    slice_filter = ""
    if slice_id:
        slice_filter = f"""
        INNER JOIN data_slice_record_association dsra
            ON rla.record_id = dsra.record_id AND rla.project_id = dsra.project_id AND dsra.data_slice_id = '{slice_id}' """

    return f"""
    WITH labels AS (
        SELECT ltl.name 
        FROM labeling_task_label ltl
        WHERE ltl.project_id = '{project_id}' AND ltl.labeling_task_id = '{labeling_task_id}'
        UNION ALL SELECT '{OUTSIDE_MARKER}' ),

    relevant_labels AS(
        SELECT rla.record_id,rla.source_type, ltl.name,rlat.token_index,rats.num_token
        FROM record_label_association rla {slice_filter}
        INNER JOIN record_label_association_token rlat
            ON rlat.record_label_association_id = rla.id
        INNER JOIN labeling_task_label ltl
            ON rla.project_id = ltl.project_id AND rla.labeling_task_label_id = ltl.id
        INNER JOIN labeling_task lt
            ON ltl.project_id = lt.project_id AND lt.id = ltl.labeling_task_id
        INNER JOIN record_attribute_token_statistics rats
            ON rla.record_id = rats.record_id AND lt.attribute_id = rats.attribute_id
        WHERE lt.id = '{labeling_task_id}' 
            AND (rla.source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}' 
                OR (rla.source_type = '{enums.LabelSource.MANUAL.value}' AND rla.is_valid_manual_label) )
            AND rla.project_id = '{project_id}'
            ),
    relevant_records AS(
        SELECT rl.record_id
        FROM relevant_labels rl
        INNER JOIN relevant_labels rl2
        	ON rl.record_id = rl2.record_id
        WHERE rl.source_type = '{enums.LabelSource.MANUAL.value}' AND rl2.source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}'
        GROUP BY rl.record_id ),
    manual AS(
        SELECT rl.*
        FROM relevant_labels rl
        INNER JOIN relevant_records r
            ON rl.record_id = r.record_id
        WHERE source_type = '{enums.LabelSource.MANUAL.value}'),
    weak AS(
        SELECT rl.*
        FROM relevant_labels rl
        INNER JOIN relevant_records r
            ON rl.record_id = r.record_id
        WHERE source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}'),
    base_matrix AS (
    SELECT manual_label, ws_label, COUNT(*)c
    FROM (
        SELECT COALESCE(m.name,'{OUTSIDE_MARKER}') manual_label,COALESCE(w.name,'{OUTSIDE_MARKER}') ws_label, COALESCE(m.token_index,w.token_index) token_index, COALESCE(m.num_token,w.num_token) num_token, COALESCE(m.record_id,w.record_id) record_id
        FROM manual m
        FULL OUTER JOIN weak w
            ON m.record_id = w.record_id AND m.token_index = w.token_index )x
    GROUP BY manual_label, ws_label ),
    max_token AS (
        SELECT SUM(num_token) all_outside
        FROM record_attribute_token_statistics rats
        INNER JOIN relevant_records rr
            ON rats.record_id = rr.record_id
        INNER JOIN labeling_task lt
            ON lt.attribute_id = rats.attribute_id
        WHERE lt.project_id = '{project_id}' AND lt.id = '{labeling_task_id}')


    SELECT array_agg(row_to_json(x))
    FROM (
        SELECT base.manual_label label_name_manual,base.ws_label label_name_ws, COALESCE(v.c,0) count_absolute
        FROM (
            SELECT l1.name manual_label, l2.name ws_label
            FROM labels l1,labels l2
        ) base
        LEFT JOIN (
            SELECT *
            FROM base_matrix
            UNION ALL
            SELECT '{OUTSIDE_MARKER}', '{OUTSIDE_MARKER}', mt.all_outside - bm.c c
            FROM (
                SELECT SUM(c)c
                FROM base_matrix) bm,max_token mt 
        ) v
            ON base.manual_label = v.manual_label AND base.ws_label = v.ws_label)x  """


def __build_sql_confusion_matrix_classification(
    project_id: str,
    labeling_task_id: str,
    slice_id: Optional[str] = None,
) -> str:

    slice_filter = ""
    if slice_id:
        slice_filter = f"""
        INNER JOIN data_slice_record_association dsra
            ON rla.record_id = dsra.record_id AND rla.project_id = dsra.project_id AND dsra.data_slice_id = '{slice_id}' """

    return f"""
    WITH record_labels AS(
    SELECT rla.record_id, rla.labeling_task_label_id label_id, rla.source_type
    FROM record_label_association rla
    INNER JOIN labeling_task_label ltl
        ON rla.labeling_task_label_id = ltl.id AND rla.project_id = ltl.project_id AND ltl.labeling_task_id = '{labeling_task_id}' {slice_filter}
    WHERE rla.project_id = '{project_id}' AND (rla.source_type = '{enums.LabelSource.WEAK_SUPERVISION.value}' 
        OR (rla.source_type = '{enums.LabelSource.MANUAL.value}' AND rla.is_valid_manual_label)) 
    GROUP BY rla.record_id, rla.labeling_task_label_id,rla.source_type ),
    label_matrix AS(
        SELECT ltl.id label_id_manual, ltl.name label_name_manual,ltl2.id label_id_ws,ltl2.name label_name_ws
        FROM labeling_task_label ltl
        INNER JOIN labeling_task_label ltl2
            ON ltl.project_id = ltl2.project_id AND ltl.labeling_task_id = ltl2.labeling_task_id
        WHERE ltl.project_id = '{project_id}' AND ltl.labeling_task_id = '{labeling_task_id}')

    SELECT array_agg(row_to_json(x))
    FROM (
        SELECT lm.*, COALESCE(x.count_absolute,0) count_absolute
        FROM label_matrix lm
        LEFT JOIN (
            SELECT rl.label_id label_id_manual, rl2.label_id label_id_ws,COUNT(*) count_absolute
            FROM record_labels rl
            INNER JOIN record_labels rl2
                ON rl.record_id = rl2.record_id AND rl.source_type != rl2.source_type
            WHERE rl.source_type = '{enums.LabelSource.MANUAL.value}'
            GROUP BY rl.label_id, rl2.label_id
        ) x
            ON lm.label_id_manual = x.label_id_manual AND lm.label_id_ws = x.label_id_ws)x
    """


def is_rats_tokenization_still_running(project_id: str) -> bool:
    query = f"""
    SELECT rtt.type, rtt.state
    FROM record_tokenization_task rtt
    WHERE rtt.project_id = '{project_id}'
    ORDER BY rtt.started_at DESC
    LIMIT 1
    """
    values = general.execute_first(query)
    if not values:
        # e.g. at the very start of a project no entry exists yet
        return True
    if (
        values[0] == enums.TokenizerTask.TYPE_TOKEN_STATISTICS.value
        and values[1] == enums.TokenizerTask.STATE_FINISHED.value
    ):
        return False
    else:
        return True


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
