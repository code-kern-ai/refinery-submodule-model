from datetime import datetime
from sqlalchemy import cast, TEXT
from typing import Dict, List, Any, Optional

from submodules.model import enums

from . import general
from .. import InformationSourceStatisticsExclusion
from ..models import (
    InformationSource,
    InformationSourcePayload,
    InformationSourceStatistics,
)
from ..session import session
from ..util import prevent_sql_injection


def get(project_id: str, source_id: str) -> InformationSource:
    return (
        session.query(InformationSource)
        .filter(
            InformationSource.project_id == project_id,
            InformationSource.id == source_id,
        )
        .first()
    )


def get_by_name(project_id: str, name: str) -> InformationSource:
    return (
        session.query(InformationSource)
        .filter(
            InformationSource.project_id == project_id,
            InformationSource.name == name,
        )
        .first()
    )


def get_all(project_id: str) -> List[InformationSource]:
    return (
        session.query(InformationSource)
        .filter(
            InformationSource.project_id == project_id,
        )
        .all()
    )


def get_all_ids_by_labeling_task_id(
    project_id: str, labeling_task_id: str
) -> List[str]:
    values = (
        session.query(cast(InformationSource.id, TEXT))
        .filter(
            InformationSource.project_id == project_id,
            InformationSource.labeling_task_id == labeling_task_id,
        )
        .all()
    )
    return [value[0] for value in values]


def get_all_statistics(project_id: str) -> List[InformationSourceStatistics]:
    return (
        session.query(InformationSourceStatistics)
        .filter(InformationSourceStatistics.project_id == project_id)
        .all()
    )


def get_payload(project_id: str, payload_id: str) -> InformationSourcePayload:
    return (
        session.query(InformationSourcePayload)
        .filter(
            InformationSourcePayload.project_id == project_id,
            InformationSourcePayload.id == payload_id,
        )
        .first()
    )


def get_last_payload(project_id: str, source_id: str) -> InformationSourcePayload:
    return (
        session.query(InformationSourcePayload)
        .filter(
            InformationSourcePayload.project_id == project_id,
            InformationSourcePayload.source_id == source_id,
        )
        .order_by(InformationSourcePayload.created_at.desc())
        .first()
    )


def get_selected_information_sources(project_id: str) -> str:
    information_sources = (
        session.query(InformationSource.name)
        .filter(
            InformationSource.project_id == project_id,
            InformationSource.is_selected == True,
        )
        .all()
    )
    if not information_sources:
        return ""
    return ", ".join([str(x.name) for x in information_sources])


def get_payloads_by_project_id(project_id: str) -> List[Any]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query: str = f"""
        SELECT 
            payload.id,
            payload.source_id,
            payload.created_at,
            payload.finished_at,
            payload.iteration,
            payload.source_code,
            payload.logs,
            payload.state
        FROM 
            information_source_payload AS payload
        INNER JOIN
            information_source 
        ON 
            payload.source_id=information_source.id
        WHERE 
            information_source.project_id='{project_id}'
        ;
        """
    return general.execute_all(query)


def get_exclusion_record_ids(source_id: str) -> List[str]:
    exclusions = (
        session.query(InformationSourceStatisticsExclusion.record_id).filter(
            InformationSourceStatisticsExclusion.source_id == source_id
        )
    ).all()
    return [str(exclusion.record_id) for exclusion in exclusions]


def get_exclusion_record_ids_for_task(task_id: str) -> List[str]:
    exclusions = (
        session.query(InformationSourceStatisticsExclusion.record_id).filter(
            InformationSourceStatisticsExclusion.source_id == InformationSource.id,
            InformationSource.labeling_task_id == task_id,
        )
    ).all()
    exclusion_ids = [str(exclusion) for exclusion, in exclusions]
    return exclusion_ids


def get_all_states(project_id: str, source_id: Optional[str] = None) -> Dict[str, str]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    source_id = prevent_sql_injection(source_id, isinstance(source_id, str))
    source_add = ""
    if source_id:
        source_add = f" AND _is.id = '{source_id}'"
    query = f"""
    SELECT 
        _is.id::TEXT,
        isp.state state
    FROM information_source _is
    LEFT JOIN LATERAL(
        SELECT isp.id,isp.state,isp.created_at
        FROM information_source_payload isp
        WHERE _is.id = isp.source_id 
        AND _is.project_id = isp.project_id
        ORDER BY isp.iteration DESC
        LIMIT 1
    )isp ON TRUE
    WHERE _is.project_id = '{project_id}' {source_add} """

    return {r[0]: r[1] for r in general.execute_all(query)}


def get_overview_data(project_id: str) -> List[Dict[str, Any]]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    query = f"""
    SELECT array_agg(row_to_json(data_select))
    FROM (
        SELECT 
            _is.id,
            _is.name,
            _is.type "informationSourceType",
            _is.description,
            _is.is_selected selected,
            _is.created_at "createdAt",
            _is.created_by "createdBy",
            _is.labeling_task_id "labelingTaskId",
            _is.return_type "returnType",
            COALESCE(queue.state,isp.state) state,
            isp.created_at "lastRun",
            stats.stat_data
        FROM information_source _is
        LEFT JOIN LATERAL(
            SELECT isp.id,isp.state,isp.created_at
            FROM information_source_payload isp
            WHERE _is.id = isp.source_id 
            AND _is.project_id = isp.project_id
            ORDER BY isp.iteration DESC
            LIMIT 1
        )isp ON TRUE
        LEFT JOIN (
            SELECT (task_info ->> 'project_id')::uuid AS project_id, task_info ->> 'information_source_id' is_id, 'QUEUED' state
            FROM global.task_queue tq
            WHERE NOT tq.is_active AND (task_info ->> 'project_id') = '{project_id}' AND tq.task_type = '{enums.TaskType.INFORMATION_SOURCE.value}'
        ) queue
            ON queue.project_id = _is.project_id AND _is.id::TEXT = queue.is_id
        LEFT JOIN (
            SELECT source_id, array_agg(row_to_json(data_select.*)) stat_data
            FROM (
                SELECT iss.source_id, iss.id, iss.true_positives,iss.false_positives,iss.false_negatives,iss.record_coverage,iss.total_hits,iss.source_conflicts,iss.source_overlaps,ltl.id "labelId",ltl.name "label",ltl.color color
                FROM information_source_statistics iss
                INNER JOIN labeling_task_label ltl
                    ON iss.project_id = ltl.project_id AND iss.labeling_task_label_id=ltl.id
                WHERE iss.project_id = '{project_id}' ) data_select
            GROUP BY source_id) stats
            ON _is.id = stats.source_id
        WHERE _is.project_id = '{project_id}'
        ORDER BY "createdAt" DESC,name
        )data_select """
    values = general.execute_first(query)

    if values:
        return values[0]


def continue_payload(project_id: str, source_id: str, payload_id: str) -> bool:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    source_id = prevent_sql_injection(source_id, isinstance(source_id, str))
    payload_id = prevent_sql_injection(payload_id, isinstance(payload_id, str))

    query = f"""
    SELECT isp.state
    FROM information_source_payload isp
    INNER JOIN information_source _is
        ON isp.source_id = _is.id AND isp.project_id = _is.project_id
    WHERE isp.id = '{payload_id}' 
    AND isp.source_id = '{source_id}' 
    AND isp.project_id = '{project_id}' """

    value = general.execute_first(query)
    if not value or value[0] != "CREATED":
        return False
    return True


def create(
    project_id: str,
    name: str,
    labeling_task_id: str,
    type: str,
    return_type: str,
    description: str,
    source_code: str,
    is_selected: Optional[bool] = None,
    version: Optional[int] = None,
    created_at: Optional[datetime] = None,
    created_by: Optional[str] = None,
    with_commit: bool = False,
) -> InformationSource:
    information_source: InformationSource = InformationSource(
        project_id=project_id,
        labeling_task_id=labeling_task_id,
        name=name,
        type=type,
        return_type=return_type,
        description=description,
        source_code=source_code,
        is_selected=is_selected,
        version=version,
        created_at=created_at,
        created_by=created_by,
    )
    general.add(information_source, with_commit)
    return information_source


def create_payload(
    project_id: str,
    source_id: str,
    state: str,
    created_by: Optional[str] = None,
    created_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    iteration: Optional[int] = None,
    source_code: Optional[str] = None,
    logs: List[str] = None,
    with_commit: bool = False,
) -> InformationSourcePayload:
    payload: InformationSourcePayload = InformationSourcePayload(
        source_id=source_id, project_id=project_id, state=state
    )
    if created_by:
        payload.created_by = created_by
    if iteration:
        payload.iteration = iteration
    if source_code:
        payload.source_code = source_code
    if created_at:
        payload.created_at = created_at
    if finished_at:
        payload.finished_at = finished_at
    if logs:
        payload.logs = logs
    general.add(payload, with_commit)
    return payload


def create_statistics(
    project_id: str,
    source_id: str,
    labeling_task_label_id: str,
    true_positives: Optional[int] = None,
    false_positives: Optional[int] = None,
    false_negatives: Optional[int] = None,
    record_coverage: Optional[int] = None,
    total_hits: Optional[int] = None,
    source_conflicts: Optional[int] = None,
    source_overlaps: Optional[int] = None,
    with_commit: bool = False,
) -> InformationSourceStatistics:
    statistics: InformationSourceStatistics = InformationSourceStatistics(
        project_id=project_id,
        source_id=source_id,
        labeling_task_label_id=labeling_task_label_id,
        true_positives=true_positives,
        false_positives=false_positives,
        false_negatives=false_negatives,
        record_coverage=record_coverage,
        total_hits=total_hits,
        source_conflicts=source_conflicts,
        source_overlaps=source_overlaps,
    )
    general.add(statistics, with_commit)
    return statistics


def delete(project_id: str, source_id: str, with_commit: bool = False) -> None:
    session.query(InformationSource).filter(
        InformationSource.project_id == project_id,
        InformationSource.id == source_id,
    ).delete()
    general.flush_or_commit(with_commit)


def update(
    project_id: str,
    source_id: str,
    labeling_task_id: Optional[str] = None,
    name: Optional[str] = None,
    type: Optional[str] = None,
    return_type: Optional[str] = None,
    description: Optional[str] = None,
    source_code: Optional[str] = None,
    is_selected: Optional[bool] = None,
    version: Optional[int] = None,
    created_at: Optional[datetime] = None,
    created_by: Optional[str] = None,
    with_commit: bool = False,
) -> InformationSource:
    information_source = get(project_id, source_id)

    if labeling_task_id is not None:
        information_source.labeling_task_id = labeling_task_id
    if name is not None:
        information_source.name = name
    if type is not None:
        information_source.type = type
    if return_type is not None:
        information_source.return_type = return_type
    if description is not None:
        information_source.description = description
    if source_code is not None:
        information_source.source_code = source_code
    if is_selected is not None:
        information_source.is_selected = is_selected
    if version is not None:
        information_source.version = version
    if created_at is not None:
        information_source.created_at = created_at
    if created_by is not None:
        information_source.created_at = created_by
    general.flush_or_commit(with_commit)
    return information_source


def update_payload(
    project_id: str,
    payload_id: str,
    state: str = None,
    progress: float = None,
    with_commit: bool = False,
) -> None:
    payload_item = get_payload(project_id, payload_id)
    if not payload_item:
        return
    if state:
        payload_item.state = state
    if progress:
        payload_item.progress = progress
    general.flush_or_commit(with_commit)


def update_quality_stats(
    project_id: str,
    source_id: str,
    statistics_item: Dict[str, Dict[str, int]],
    with_commit: bool = False,
) -> None:
    for label_id, stats_dict in statistics_item.items():
        statistics = (
            session.query(InformationSourceStatistics)
            .filter(
                InformationSourceStatistics.labeling_task_label_id == label_id,
                InformationSourceStatistics.source_id == source_id,
                InformationSourceStatistics.project_id == project_id,
            )
            .first()
        )
        if statistics is not None:
            statistics.true_positives = stats_dict["true_positives"]
            statistics.false_positives = stats_dict["false_positives"]
            statistics.false_negatives = stats_dict["false_negatives"]
    general.flush_or_commit(with_commit)


def update_quantity_stats(
    project_id: str,
    source_id: str,
    statistics_item: Dict[str, Dict[str, int]],
    with_commit: bool = False,
) -> None:
    for label_id, stats_dict in statistics_item.items():
        statistics = (
            session.query(InformationSourceStatistics)
            .filter(
                InformationSourceStatistics.labeling_task_label_id == label_id,
                InformationSourceStatistics.source_id == source_id,
                InformationSourceStatistics.project_id == project_id,
            )
            .first()
        )
        if statistics is None:
            statistics = InformationSourceStatistics(
                source_id=source_id,
                labeling_task_label_id=label_id,
                record_coverage=stats_dict["record_coverage"],
                total_hits=stats_dict["total_hits"],
                source_conflicts=stats_dict["source_conflicts"],
                source_overlaps=stats_dict["source_overlaps"],
                project_id=project_id,
            )
            general.add(statistics)
        else:
            statistics.record_coverage = stats_dict["record_coverage"]
            statistics.total_hits = stats_dict["total_hits"]
            statistics.source_conflicts = stats_dict["source_conflicts"]
            statistics.source_overlaps = stats_dict["source_overlaps"]

        general.flush_or_commit(with_commit)


def update_is_selected_for_project(
    project_id: str,
    update_value: bool,
    with_commit: bool = False,
    only_with_state: Optional[enums.PayloadState] = None,
) -> None:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))

    if update_value:
        str_value = "TRUE"
    else:
        # to ensure nothing wrong gets set
        str_value = "FALSE"

    id_selection = ""
    if only_with_state:
        states = get_all_states(project_id)
        ids = [key for key in states if states[key] == only_with_state.value]
        if len(ids) == 0:
            return
        id_selection = "AND id IN ('" + "', '".join(ids) + "')"
    query = f"""
    UPDATE information_source
    SET is_selected = {str_value}
    WHERE project_id = '{project_id}'
    {id_selection}
    """
    general.execute(query)
    general.flush_or_commit(with_commit)


def toggle(project_id: str, source_id: str, with_commit: bool = False) -> None:
    # FIXME maybe this should get set explicit for idempotency
    information_source = get(project_id, source_id)
    information_source.is_selected = not information_source.is_selected
    general.flush_or_commit(with_commit)


def delete_sources_exlusion_entries(
    project_id: str, information_source_id: str, with_commit: bool = False
) -> None:
    session.query(InformationSourceStatisticsExclusion).filter(
        InformationSourceStatisticsExclusion.source_id == information_source_id,
        InformationSourceStatisticsExclusion.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)


def delete_stats(project_id: str, source_id: str, with_commit: bool = False) -> None:
    session.query(InformationSourceStatistics).filter(
        InformationSourceStatistics.source_id == source_id,
        InformationSourceStatistics.project_id == project_id,
    ).delete()
    general.flush_or_commit(with_commit)


def check_is_active(project_id: str, statistics_id: str) -> bool:
    return (
        session.query(InformationSourceStatistics.record_coverage)
        .filter(
            InformationSourceStatistics.id == statistics_id,
            InformationSourceStatistics.project_id == project_id,
        )
        .first()[0]
        > 0
    )


def get_source_statistics(
    project_id: str, heuristic_id: str
) -> List[InformationSourceStatistics]:
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    heuristic_id = prevent_sql_injection(heuristic_id, isinstance(heuristic_id, str))

    query = f"""
        SELECT iss.id, iss.true_positives, iss.false_negatives, iss.false_positives, iss.record_coverage, iss.total_hits, iss.source_conflicts,json_build_object('name', ltl.name, 'color', ltl.color,'id', ltl.id) AS labeling_task_label
        FROM information_source_statistics iss
        JOIN labeling_task_label ltl 
            ON ltl.id = iss.labeling_task_label_id
        WHERE iss.project_id = '{project_id}' AND source_id = '{heuristic_id}'
    """
    return general.execute_all(query)


def get_heuristic_id_with_most_recent_payload(project_id: str, heuristic_id: str):
    project_id = prevent_sql_injection(project_id, isinstance(project_id, str))
    heuristic_id = prevent_sql_injection(heuristic_id, isinstance(heuristic_id, str))

    base_columns = general.construct_select_columns("information_source", "public", "h")
    query = f"""
    SELECT {base_columns}, row_to_json(isp) last_payload        
    FROM information_source h
    LEFT JOIN LATERAL(
        SELECT isp.id, isp.created_at, isp.finished_at, isp.state, isp.iteration, isp.progress
        FROM information_source_payload isp
        WHERE h.id = isp.source_id AND h.project_id = isp.project_id
        ORDER BY isp.iteration DESC
        LIMIT 1
    ) isp
        ON TRUE
    WHERE h.project_id = '{project_id}' AND h.id = '{heuristic_id}'
    """
    return general.execute_first(query)
