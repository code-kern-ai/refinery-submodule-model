import datetime
from typing import List, Dict, Any
from ..session import session
from submodules.model.enums import TokenSubject
from submodules.model.business_objects import general
from submodules.model.models import (
    CognitionPersonalAccessTokenEtl,
    PersonalAccessTokenScopeEtl,
)

TOKEN_WITH_SCOPE_QUERY = """SELECT
    pate.*,
    ts.scopes
FROM cognition.personal_access_token_etl pate
INNER JOIN (
    SELECT patse.token_id, array_agg(row_to_json(patse)) AS scopes
    FROM (SELECT * FROM cognition.personal_access_token_scope_etl) patse
    GROUP BY patse.token_id
) ts
    ON pate.id = ts.token_id
"""


def get(token_id: str) -> Dict[str, Any]:
    sql = TOKEN_WITH_SCOPE_QUERY + f"WHERE pate.id = '{token_id}'"
    return general.execute_first(sql)


def get_by_user_and_name(
    created_by: str,
    name: str,
) -> CognitionPersonalAccessTokenEtl:
    return (
        session.query(CognitionPersonalAccessTokenEtl)
        .filter(
            CognitionPersonalAccessTokenEtl.name == name,
            CognitionPersonalAccessTokenEtl.created_by == created_by,
        )
        .first()
    )


def get_all(org_id: str) -> List[Dict[str, Any]]:
    sql = TOKEN_WITH_SCOPE_QUERY + f"WHERE pate.organization_id = '{org_id}'"
    return general.execute_all(sql)


def get_all_by_user(user_id: str) -> List[Dict[str, Any]]:
    sql = TOKEN_WITH_SCOPE_QUERY + f"WHERE pate.created_by = '{user_id}'"
    return general.execute_all(sql)


def get_by_token(token: str) -> CognitionPersonalAccessTokenEtl:
    return (
        session.query(CognitionPersonalAccessTokenEtl)
        .filter(
            CognitionPersonalAccessTokenEtl.token == token,
        )
        .first()
    )


def get_token_scopes(token_id: str) -> List[PersonalAccessTokenScopeEtl]:
    return (
        session.query(PersonalAccessTokenScopeEtl)
        .filter(PersonalAccessTokenScopeEtl.token_id == token_id)
        .all()
    )


def __get_or_create(
    org_id: str,
    name: str,
    token: str,
    expires_at: datetime,
    created_by: str,
    with_commit: bool = False,
) -> CognitionPersonalAccessTokenEtl:
    # only used with create function from this module
    # in order to avoid duplicate tokens when adding scopes
    pat = get_by_user_and_name(created_by, name)
    if not pat:
        pat = CognitionPersonalAccessTokenEtl(
            organization_id=org_id,
            name=name,
            token=token,
            expires_at=expires_at,
            created_by=created_by,
        )
        general.add(pat, with_commit)
    return pat


def create(
    org_id: str,
    created_by: str,
    name: str,
    scope: str,
    expires_at: datetime,
    token: str,
    subject: str = TokenSubject.PROJECT.value,
    with_commit: bool = False,
) -> CognitionPersonalAccessTokenEtl:
    pat = __get_or_create(
        org_id=org_id,
        name=name,
        token=token,
        expires_at=expires_at,
        created_by=created_by,
        with_commit=False,
    )
    pat_scope = PersonalAccessTokenScopeEtl(
        scope=scope,
        subject=subject,
    )
    pat.scopes.append(pat_scope)
    general.add(pat, with_commit)
    return pat


def delete(
    token_id: str,
    with_commit: bool = False,
) -> None:
    session.query(CognitionPersonalAccessTokenEtl).filter(
        CognitionPersonalAccessTokenEtl.id == token_id
    ).delete()
    general.flush_or_commit(with_commit)


def delete_many(
    token_ids: List[str],
    with_commit: bool = False,
) -> None:
    session.query(CognitionPersonalAccessTokenEtl).filter(
        CognitionPersonalAccessTokenEtl.id.in_(token_ids),
    ).delete()
    general.flush_or_commit(with_commit)


def update_last_used(
    token_id: str,
    with_commit: bool = False,
) -> None:
    token_item = (
        session.query(CognitionPersonalAccessTokenEtl)
        .filter(
            CognitionPersonalAccessTokenEtl.id == token_id,
        )
        .first()
    )

    token_item.last_used = datetime.datetime.now()
    general.flush_or_commit(with_commit)
