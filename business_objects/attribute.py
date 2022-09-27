from typing import Dict, Any, List, Optional
from sqlalchemy import func
from sqlalchemy.orm.attributes import flag_modified

from . import general
from ..enums import AttributeState, DataTypes, RecordCategory
from ..models import Attribute
from ..session import session


def get(project_id: str, attribute_id: str) -> Attribute:
    return (
        session.query(Attribute)
        .filter(Attribute.project_id == project_id, Attribute.id == attribute_id)
        .first()
    )


def get_data_type(project_id: str, name: str) -> str:
    data_type: Any = (
        session.query(Attribute.data_type)
        .filter(
            Attribute.project_id == project_id,
            Attribute.name == name,
        )
        .first()
    )
    return data_type.data_type if data_type else data_type


def get_by_name(project_id: str, name: str) -> Attribute:
    return (
        session.query(Attribute)
        .filter(
            Attribute.project_id == project_id,
            Attribute.name == name,
        )
        .first()
    )


def get_all_by_names(project_id: str, attribute_names: List[str]) -> List[Attribute]:
    return (
        session.query(Attribute)
        .filter(
            Attribute.project_id == project_id,
            Attribute.name.in_(attribute_names),
        )
        .all()
    )


def get_all(
    project_id: Optional[str] = None,
    state_filter: List[str] = [
        AttributeState.UPLOADED.value,
        AttributeState.USABLE.value,
        AttributeState.AUTOMATICALLY_CREATED.value,
    ],
) -> List[Attribute]:
    query = session.query(Attribute)
    if project_id:
        query = query.filter(Attribute.project_id == project_id)
    if state_filter:
        query = query.filter(Attribute.state.in_(state_filter))
    return query.all()


def get_attribute_ids(
    project_id: str,
    state_filter: List[str] = [
        AttributeState.UPLOADED.value,
        AttributeState.USABLE.value,
        AttributeState.AUTOMATICALLY_CREATED.value,
    ],
) -> Dict[str, str]:
    attributes: List[Attribute] = get_all(project_id, state_filter)
    return {attribute.name: attribute.id for attribute in attributes}


def get_text_attributes(
    project_id: str,
    state_filter: List[str] = [
        AttributeState.UPLOADED.value,
        AttributeState.USABLE.value,
        AttributeState.AUTOMATICALLY_CREATED.value,
    ],
) -> Dict[str, str]:
    query = session.query(Attribute).filter(
        Attribute.project_id == project_id, Attribute.data_type == "TEXT"
    )
    if state_filter:
        query = query.filter(Attribute.state.in_(state_filter))
    text_attributes = query.all()
    return {att.name: str(att.id) for att in text_attributes}


def get_all_ordered(
    project_id: str,
    order_asc: bool,
    state_filter: List[str] = [
        AttributeState.UPLOADED.value,
        AttributeState.USABLE.value,
        AttributeState.AUTOMATICALLY_CREATED.value,
    ],
) -> List[Attribute]:
    query = session.query(Attribute).filter(Attribute.project_id == project_id)
    if state_filter:
        query = query.filter(Attribute.state.in_(state_filter))
    if order_asc:
        query = query.order_by(Attribute.relative_position.asc())
    else:
        query = query.order_by(Attribute.relative_position)
    return query.all()


def get_relative_position(project_id: str) -> int:
    result = (
        session.query(func.max(Attribute.relative_position))
        .filter(Attribute.project_id == project_id)
        .first()
    )

    return result[0] if result else None


def get_primary_keys(project_id: str) -> List[Attribute]:
    return (
        session.query(Attribute)
        .filter(Attribute.project_id == project_id, Attribute.is_primary_key == True)
        .all()
    )


def get_unique_attributes_count(project_id: str) -> int:
    return (
        session.query(Attribute)
        .filter(Attribute.project_id == project_id, Attribute.is_primary_key == True)
        .count()
    )


def create(
    project_id: str,
    name: str,
    relative_position: int,
    data_type: str = DataTypes.CATEGORY.value,
    is_primary_key: bool = False,
    user_created: bool = False,
    source_code: Optional[str] = None,
    state: Optional[str] = None,
    logs: Optional[List[str]] = None,
    with_commit: bool = False,
) -> Attribute:
    attribute: Attribute = Attribute(
        project_id=project_id,
        name=name,
        data_type=data_type,
        is_primary_key=is_primary_key,
        relative_position=relative_position,
        user_created=user_created,
    )

    if source_code is not None:
        attribute.source_code = source_code

    if state is not None:
        attribute.state = state

    if logs is not None:
        attribute.logs = logs

    general.add(attribute, with_commit)
    return attribute


def update(
    project_id: str,
    attribute_id: str,
    data_type: Optional[str] = None,
    is_primary_key: Optional[bool] = None,
    name: Optional[str] = None,
    source_code: Optional[str] = None,
    state: Optional[str] = None,
    logs: Optional[List[str]] = None,
    with_commit: bool = False,
) -> Attribute:
    attribute: Attribute = get(project_id, attribute_id)
    if data_type is not None:
        attribute.data_type = data_type
    if is_primary_key is not None:
        attribute.is_primary_key = is_primary_key
    if name is not None:
        attribute.name = name
    if source_code is not None:
        attribute.source_code = source_code
    if state is not None:
        attribute.state = state
    if logs is not None:
        attribute.logs = logs
        flag_modified(attribute, "logs")
    general.flush_or_commit(with_commit)
    return attribute


def delete(project_id: str, attribute_id: str, with_commit: bool = False) -> None:
    session.query(Attribute).filter(
        Attribute.project_id == project_id,
        Attribute.id == attribute_id,
    ).delete()
    general.flush_or_commit(with_commit)


def check_composite_key_is_valid(project_id: str) -> bool:
    primary_keys: List[Attribute] = get_primary_keys(project_id)

    if not primary_keys:
        return True

    def __build_inner_sql(
        project_id: str, primary_keys: List[Attribute], category: str
    ) -> str:
        sql_attributes: str = ""
        for primary_key in primary_keys:
            sql_attributes: str = (
                f"r.data->>'{primary_key.name}'"
                if not sql_attributes
                else f"{sql_attributes}, r.data->>'{primary_key.name}'"
            )

        return f"SELECT COUNT(*) AS c FROM record r WHERE r.project_id = '{project_id}' AND r.category = '{category}' GROUP BY {sql_attributes}"

    def __build_sql(
        project_id: str, primary_keys: List[Attribute], category: str
    ) -> str:
        inner_sql = __build_inner_sql(project_id, primary_keys, category)
        return f"SELECT * FROM ({inner_sql}) AS query WHERE c != 1"

    is_valid_scale = (
        general.execute_first(
            __build_sql(project_id, primary_keys, RecordCategory.SCALE.value)
        )
        is None
    )
    is_valid_test = (
        general.execute_first(
            __build_sql(project_id, primary_keys, RecordCategory.TEST.value)
        )
        is None
    )
    is_valid = is_valid_scale and is_valid_test

    return is_valid


def add_running_id(
    project_id: str,
    attribute_name: str,
    for_retokenization: bool = True,
    with_commit: bool = False,
) -> None:
    general.execute(__build_add_query(project_id, attribute_name, for_retokenization))
    general.flush_or_commit(with_commit)


def __build_add_query(
    project_id: str, attribute_name: str, for_retokenization: bool
) -> str:
    current_attributes = get_all_ordered(project_id, True)
    json_cols = ",\n".join(
        [f"'{att.name}', \"data\"->'{att.name}'" for att in current_attributes]
    )
    if for_retokenization:
        remove_query = f"""
        DELETE FROM record_tokenized
        WHERE project_id = '{project_id}' AND id IN (
            SELECT id
            FROM record_tokenized
            WHERE project_id = '{project_id}'
            LIMIT 1 );
        """
    else:
        remove_query = ""
    return (
        f"""
    UPDATE record
    SET "data" = helper.dd
    FROM (
        SELECT  r.id,
            json_build_object(
            '{attribute_name}', ROW_NUMBER () OVER(),
            {json_cols}) dd
        FROM record r 
        WHERE project_id = '{project_id}') helper
    WHERE record.project_id = '{project_id}' AND record.id = helper.id;

    INSERT INTO attribute
    VALUES (uuid_in(md5(random()::TEXT || clock_timestamp()::TEXT)::CSTRING),'{project_id}','{attribute_name}','INTEGER',TRUE,0,FALSE,NULL,'{AttributeState.AUTOMATICALLY_CREATED.value}',NULL);

    UPDATE attribute
    SET relative_position = rPos
    FROM (
        SELECT a.id, ROW_NUMBER () OVER(ORDER BY relative_position) rPos
        FROM attribute a
        WHERE a.project_id = '{project_id}'
    ) helper
    WHERE attribute.project_id = '{project_id}' AND attribute.id = helper.id;"""
        + remove_query
    )
