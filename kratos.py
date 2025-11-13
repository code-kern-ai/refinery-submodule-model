from email.mime.text import MIMEText
import smtplib
from typing import Union, Any, List, Dict
import os
import requests
import logging
from datetime import datetime, timedelta
from urllib.parse import quote

from submodules.model import daemon
from submodules.model.business_objects import general, user


logging.basicConfig(level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

KRATOS_ADMIN_URL = os.getenv("KRATOS_ADMIN_URL")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = os.getenv("SMTP_PORT")
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# user_id -> {"identity" -> full identity, "simple" -> {"id": str, "mail": str, "firstName": str, "lastName": str}}
# "collected" -> timestamp
KRATOS_IDENTITY_CACHE: Dict[str, Any] = {}
KRATOS_IDENTITY_CACHE_TIMEOUT = timedelta(minutes=30)

LANGUAGE_MESSAGES = {
    "en": "Hello!\n\nClick the link to complete your account setup:\n\n",
    "de": "Hallo!\n\nKlicken Sie auf den Link, um Ihre Kontoeinrichtung abzuschließen:\n\n",
}

INVITATION_SUBJECT = "Sie sind zu unserer app eingeladen/You are invited to our app"

LANGUAGE_EXPIRATION_INFO = {
    "en": "This link can only be clicked once and is valid for 2 days. Contact your system admin if you have issues.",
    "de": "Dieser Link kann nur einmal angeklickt werden und ist 2 Tage lang gültig. Kontaktieren Sie Ihren Systemadministrator, wenn Sie Probleme haben.",
}


def get_cached_values(update_db_users: bool = True) -> Dict[str, Dict[str, Any]]:
    global KRATOS_IDENTITY_CACHE
    if not KRATOS_IDENTITY_CACHE or len(KRATOS_IDENTITY_CACHE) == 0:
        __refresh_identity_cache(update_db_users)
    elif (
        KRATOS_IDENTITY_CACHE["collected"] + KRATOS_IDENTITY_CACHE_TIMEOUT
        < datetime.now()
    ):
        __refresh_identity_cache(update_db_users)
    return KRATOS_IDENTITY_CACHE


def __refresh_identity_cache(update_db_users: bool = True) -> None:
    global KRATOS_IDENTITY_CACHE
    request = requests.get(f"{KRATOS_ADMIN_URL}/identities")
    if request.ok:
        collected = datetime.now()
        identities = request.json()

        # maybe more pages https://www.ory.sh/docs/ecosystem/api-design#pagination
        while next_link := __get_link_from_kratos_request(request):
            request = requests.get(next_link)
            if request.ok:
                identities.extend(request.json())

        KRATOS_IDENTITY_CACHE = {
            identity["id"]: {
                "identity": identity,
                "simple": __parse_identity_to_simple(identity),
            }
            for identity in identities
        }

        KRATOS_IDENTITY_CACHE["collected"] = collected
    else:
        KRATOS_IDENTITY_CACHE = {}

    if update_db_users:
        migrate_kratos_users()


def __get_link_from_kratos_request(request: requests.Response) -> str:
    # rel=next only if there is more than 1 page
    # </admin/identities?page_size=1&page_token=00000000-0000-0000-0000-000000000000>; rel="first",</admin/identities?page_size=1&page_token=08f30706-9919-4776-9018-6a56c4fa8bb9>; rel="next"
    link = request.headers.get("Link")
    if link:
        if 'rel="next"' in link:
            parts = link.split("<")
            for part in parts:
                if 'rel="next"' in part:
                    return part.split(">")[0].replace("/admin", KRATOS_ADMIN_URL)
    return None


def __get_identity(user_id: str, only_simple: bool = True) -> Dict[str, Any]:
    if not isinstance(user_id, str):
        user_id = str(user_id)
    cache = get_cached_values()
    if user_id in cache:
        if only_simple:
            return cache[user_id]["simple"]
        return cache[user_id]

    if len(user_id) == 36:
        # check not new entry outside cache
        request = requests.get(f"{KRATOS_ADMIN_URL}/identities/{user_id}")
        if request.ok:
            identity = request.json()
            if identity["id"] == user_id:
                KRATOS_IDENTITY_CACHE[user_id] = {
                    "identity": identity,
                    "simple": __parse_identity_to_simple(identity),
                }
                if only_simple:
                    return KRATOS_IDENTITY_CACHE[user_id]["simple"]
                return KRATOS_IDENTITY_CACHE[user_id]
    # e.g. if id "GOLD_STAR" is requested => wont be in cache but expects a dummy dict
    if only_simple:
        return __parse_identity_to_simple({"id": user_id})
    return {
        "identity": {
            "id": user_id,
            "traits": {"email": None, "name": {"first": None, "last": None}},
        }
    }


def __parse_identity_to_simple(identity: Dict[str, Any]) -> Dict[str, str]:
    r = {
        "id": identity["id"],
        "mail": None,
        "firstName": None,
        "lastName": None,
    }
    if "traits" in identity:
        r["mail"] = identity["traits"]["email"]
        if "name" in identity["traits"]:
            r["firstName"] = identity["traits"]["name"]["first"]
            r["lastName"] = identity["traits"]["name"]["last"]
    return r


def get_userid_from_mail(user_mail: str) -> str:
    values = get_cached_values()
    for key in values:
        if key == "collected":
            continue
        if values[key]["simple"]["mail"] == user_mail:
            return key
    # not in cached values, try search kratos
    return __search_kratos_for_user_mail(user_mail)["id"]


def __search_kratos_for_user_mail(user_mail: str) -> str:
    request = requests.get(
        f"{KRATOS_ADMIN_URL}/identities?preview_credentials_identifier_similar={quote(user_mail)}"
    )
    if request.ok:
        identities = request.json()
        for i in identities:
            if i["traits"]["email"].lower() == user_mail.lower():
                return i
    return None


def resolve_user_mail_by_id(user_id: str) -> str:
    i = __get_identity(user_id)
    if i:
        return i["mail"]
    return None


def resolve_user_name_by_id(user_id: str) -> Dict[str, str]:
    i = __get_identity(user_id, False)
    if i:
        i = i["identity"]
        return i["traits"]["name"] if "name" in i["traits"] else None
    return None


def resolve_all_user_ids(
    relevant_ids: List[str], as_list: bool = True
) -> Union[Dict[str, Dict[str, str]], List[Dict[str, str]]]:
    final = [] if as_list else {}
    for id in relevant_ids:
        i = __get_identity(id)
        if as_list:
            final.append(i)
        else:
            final[id] = i
    return final


def expand_user_mail_name(
    users: List[Dict[str, str]], user_id_key="id"
) -> List[Dict[str, str]]:
    final = []
    for user in users:
        i = __get_identity(user[user_id_key])
        user = {**user, **i}
        final.append(user)
    return final


def resolve_user_name_and_email_by_id(user_id: str) -> dict:
    i = __get_identity(user_id, False)
    if i:
        i = i["identity"]
    if i and "traits" in i and i["traits"]:
        return i["traits"]["name"], i["traits"]["email"]
    return None


def create_user_kratos(email: str, provider: str = None):
    payload_registration = {
        "schema_id": "default",
        "traits": {"email": email},
    }
    if provider:
        payload_registration["metadata_public"] = {
            "registration_scope": {
                "provider_id": provider,
                "invitation_sso": True,
            }
        }
    response_create = requests.post(
        f"{KRATOS_ADMIN_URL}/identities",
        json=payload_registration,
    )
    return response_create.json() if response_create.ok else None


def delete_user_kratos(user_id: str) -> bool:
    response_delete = requests.delete(f"{KRATOS_ADMIN_URL}/identities/{user_id}")
    if response_delete.ok:
        del KRATOS_IDENTITY_CACHE[user_id]
        return True
    return False


def get_recovery_link(user_id: str) -> str:
    payload_recovery_link = {
        "expires_in": "48h",
        "identity_id": user_id,
    }
    response_link = requests.post(
        f"{KRATOS_ADMIN_URL}/recovery/link", json=payload_recovery_link
    )
    return response_link.json() if response_link.ok else None


def email_with_link(to_email: str, recovery_link: str) -> None:
    msg = MIMEText(
        f"{LANGUAGE_MESSAGES['de']}{recovery_link}\n\n{LANGUAGE_EXPIRATION_INFO['de']}\n\n\n------\n\n{LANGUAGE_MESSAGES['en']}{recovery_link}\n\n{LANGUAGE_EXPIRATION_INFO['en']}",
    )
    msg["Subject"] = INVITATION_SUBJECT
    msg["From"] = "signup@kern.ai"
    msg["To"] = to_email

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        if SMTP_USER and SMTP_PASSWORD:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)


def send_bulk_emails(emails: List[str], recovery_links: List[str]) -> None:

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        if SMTP_USER and SMTP_PASSWORD:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)

        for to_email, recovery_link in zip(emails, recovery_links):
            msg = MIMEText(
                f"{LANGUAGE_MESSAGES['de']}{recovery_link}\n\n{LANGUAGE_EXPIRATION_INFO['de']}\n\n\n------\n\n{LANGUAGE_MESSAGES['en']}{recovery_link}\n\n{LANGUAGE_EXPIRATION_INFO['en']}",
            )
            msg["Subject"] = INVITATION_SUBJECT
            msg["From"] = "signup@kern.ai"
            msg["To"] = to_email
            server.send_message(msg)


def check_user_exists(email: str) -> bool:
    request = requests.get(
        f"{KRATOS_ADMIN_URL}/identities?preview_credentials_identifier_similar={quote(email)}"
    )
    if request.ok:
        identities = request.json()
        for i in identities:
            if i["traits"]["email"].lower() == email.lower():
                return True
    return False


def migrate_kratos_users() -> None:
    daemon.run_with_db_token(__migrate_kratos_users)


def __migrate_kratos_users():
    users_kratos = get_cached_values(False)
    users_database = user.get_all()

    for user_database in users_database:
        user_id = str(user_database.id)
        if user_id not in users_kratos or users_kratos[user_id] is None:
            continue
        user_identity = users_kratos[user_id]["identity"]
        if user_database.email != user_identity["traits"]["email"]:
            user_database.email = user_identity["traits"]["email"]
        if (
            user_database.verified
            != user_identity["verifiable_addresses"][0]["verified"]
        ):
            user_database.verified = user_identity["verifiable_addresses"][0][
                "verified"
            ]
        if (
            user_database.created_at
            != user_identity["verifiable_addresses"][0]["created_at"]
        ):
            user_database.created_at = user_identity["verifiable_addresses"][0][
                "created_at"
            ]
        if user_database.metadata_public != user_identity["metadata_public"]:
            user_database.metadata_public = user_identity["metadata_public"]
        sso_provider = (
            (
                user_identity["metadata_public"]
                .get("registration_scope", {})
                .get("provider_id", None)
            )
            if user_identity["metadata_public"]
            else None
        )
        if user_database.sso_provider != sso_provider:
            user_database.sso_provider = sso_provider

    general.commit()
