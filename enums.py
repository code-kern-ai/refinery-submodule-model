from enum import Enum
from typing import Any


class DataTypes(Enum):
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    CATEGORY = "CATEGORY"
    TEXT = "TEXT"
    EMBEDDING_LIST = "EMBEDDING_LIST"  # only for embeddings & default hidden
    UNKNOWN = "UNKNOWN"


class ProjectStatus(Enum):
    INIT_UPLOAD = "INIT_UPLOAD"
    INIT_COMPLETE = "INIT_COMPLETE"
    IN_DELETION = "IN_DELETION"
    INIT_SAMPLE_PROJECT = "INIT_SAMPLE_PROJECT"
    HIDDEN = "HIDDEN"


class RecordCategory(Enum):
    SCALE = "SCALE"
    TEST = "TEST"


class LabelSource(Enum):
    MANUAL = "MANUAL"
    # WEAK_SUPERVISION = Output of the Weak Supervision Model - ehemeals "programmatic"
    WEAK_SUPERVISION = "WEAK_SUPERVISION"
    INFORMATION_SOURCE = "INFORMATION_SOURCE"
    MODEL_CALLBACK = "MODEL_CALLBACK"


class InformationSourceType(Enum):
    LABELING_FUNCTION = "LABELING_FUNCTION"
    ACTIVE_LEARNING = "ACTIVE_LEARNING"
    PRE_COMPUTED = "PRE_COMPUTED"
    ZERO_SHOT = "ZERO_SHOT"
    CROWD_LABELER = "CROWD_LABELER"


class InformationSourceReturnType(Enum):
    RETURN = "RETURN"
    YIELD = "YIELD"


class Notification(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class PayloadState(Enum):
    CREATED = "CREATED"
    FINISHED = "FINISHED"
    FAILED = "FAILED"

    # for crowd labelers, there is a slightly different state flow
    STARTED = "STARTED"


class UserRoles(Enum):
    ENGINEER = "ENGINEER"
    EXPERT = "EXPERT"
    ANNOTATOR = "ANNOTATOR"


class MessageRoles(Enum):
    SYSTEM = "SYSTEM"
    USER = "USER"
    ASSISTANT = "ASSISTANT"


class LinkTypes(Enum):
    DATA_SLICE = "DATA_SLICE"
    HEURISTIC = "HEURISTIC"
    SESSION = "SESSION"


class Tablenames(Enum):
    APP_VERSION = "app_version"
    AGREEMENT = "agreement"
    USER = "user"
    ORGANIZATION = "organization"
    TEAM = "team"
    TEAM_MEMBER = "team_member"
    TEAM_RESOURCE = "team_resource"
    PROJECT = "project"
    NOTIFICATION = "notification"
    ATTRIBUTE = "attribute"
    LABELING_TASK = "labeling_task"
    LABELING_TASK_LABEL = "labeling_task_label"
    EMBEDDING = "embedding"
    EMBEDDING_TENSOR = "embedding_tensor"
    RECORD = "record"
    RECORD_TOKENIZED = "record_tokenized"
    RECORD_TOKENIZATION_TASK = "record_tokenization_task"
    RECORD_LABEL_ASSOCIATION = "record_label_association"
    RECORD_LABEL_ASSOCIATION_TOKEN = "record_label_association_token"
    RECORD_ATTRIBUTE_TOKEN_STATISTICS = "record_attribute_token_statistics"
    WEAK_SUPERVISION_TASK = "weak_supervision_task"
    WEAK_SUPERVISION_HELPER = "weak_supervision_helper"
    INFORMATION_SOURCE = "information_source"
    INFORMATION_SOURCE_STATISTICS = "information_source_statistics"
    INFORMATION_SOURCE_PAYLOAD = "information_source_payload"
    KNOWLEDGE_BASE = "knowledge_base"
    KNOWLEDGE_TERM = "knowledge_term"
    USER_SESSIONS = "user_sessions"
    USER_ACTIVITY = "user_activity"
    UPLOAD_TASK = "upload_task"
    DATA_SLICE = "data_slice"
    DATA_SLICE_RECORD_ASSOCIATION = "data_slice_record_association"
    INFORMATION_SOURCE_STATISTICS_EXCLUSION = "information_source_statistics_exclusion"
    COMMENT_DATA = "comment_data"
    LABELING_ACCESS_LINK = "labeling_access_link"
    PERSONAL_ACCESS_TOKEN = "personal_access_token"
    ADMIN_MESSAGE = "admin_message"
    TASK_QUEUE = "task_queue"
    CONVERSATION = "conversation"
    MESSAGE = "message"
    STRATEGY = "strategy"
    STRATEGY_STEP = "strategy_step"
    RETRIEVER = "retriever"
    RETRIEVER_PART = "retriever_part"
    ENVIRONMENT_VARIABLE = "environment_variable"
    PIPELINE_LOGS = "pipeline_logs"
    MARKDOWN_FILE = "markdown_file"
    REFINERY_SYNCHRONIZATION_TASK = "refinery_synchronization_task"
    PYTHON_STEP = "python_step"
    LLM_STEP = "llm_step"
    MARKDOWN_LLM_LOGS = "markdown_llm_logs"
    MARKDOWN_DATASET = "markdown_dataset"
    WEBSOCKET_ACCESS = "websocket_access"
    CONSUMPTION_LOG = "consumption_log"
    CONSUMPTION_SUMMARY = "consumption_summary"
    MACRO = "macro"  # general definition
    MACRO_NODE = "macro_node"  # step/action of a macro
    MACRO_EDGE = "macro_edge"  # connection between steps of a macro
    MACRO_EXECUTION = "macro_execution"  # links macro id to an execution id
    MACRO_EXECUTION_LINK = "macro_execution_link"  # execution to a conversation id
    CUSTOMER_BUTTON = "customer_button"

    def snake_case_to_pascal_case(self):
        # the type name (written in PascalCase) of a table is needed to create backrefs
        return "".join([word.title() for word in self.value.split("_")])

    def snake_case_to_camel_case(self):
        return "".join(
            [
                word.title() if idx > 0 else word.lower()
                for idx, word in enumerate(self.value.split("_"))
            ]
        )


class TeamResourceType(Enum):
    COGNITION_PROJECT = "COGNITION_PROJECT"


class CommentCategory(Enum):
    UNKNOWN = "unknown"
    LABELING_TASK = "LABELING_TASK"
    RECORD = "RECORD"
    ORGANIZATION = "ORGANIZATION"
    ATTRIBUTE = "ATTRIBUTE"
    USER = "USER"
    EMBEDDING = "EMBEDDING"
    HEURISTIC = "HEURISTIC"
    DATA_SLICE = "DATA_SLICE"
    KNOWLEDGE_BASE = "KNOWLEDGE_BASE"
    LABEL = "LABEL"

    def get_name_col(self):
        if self == CommentCategory.USER:
            return ""
        return "name"

    def get_table_name(self):
        if self == CommentCategory.USER:
            return "public.user"
        if self == CommentCategory.HEURISTIC:
            return "information_source"
        if self == CommentCategory.LABEL:
            return "labeling_task_label"
        return self.value.lower()


class CascadeBehaviour(Enum):
    KEEP_PARENT_ON_CHILD_DELETION = "KEEP_PARENT_ON_CHILD_DELETION"
    DELETE_BOTH_IF_EITHER_IS_DELETED = "DELETE_BOTH_IF_EITHER_IS_DELETED"


class LabelingTaskType(Enum):
    CLASSIFICATION = "MULTICLASS_CLASSIFICATION"
    INFORMATION_EXTRACTION = "INFORMATION_EXTRACTION"


class LabelingTaskTarget(Enum):
    ON_ATTRIBUTE = "ON_ATTRIBUTE"
    ON_WHOLE_RECORD = "ON_WHOLE_RECORD"


class EmbeddingType(Enum):
    ON_ATTRIBUTE = "ON_ATTRIBUTE"
    ON_TOKEN = "ON_TOKEN"


class ConfusionMatrixElements(Enum):
    OUTSIDE = "Outside"


class UploadStates(Enum):
    """
    Explanation of the differences between PENDING and WAITING:

    WAITING is used if there is some time to wait when one service is done and another is to be started.
    PENDING is used if there is some time to wait in one service.

    An transfer consits of multiple sub processesses
    Example:
        Get-Credentials-Subprocess:
            Frontend requests credentials. The gateway creates a new UploadTask with state CREATED.
            After generating the credentials the state is set to WAITING and the credentials are returned.
        MinIO-Upload-Subprocess
            The Frontend uploads the data to MinIO and MinIO notifies the gateway about the successfull transfer.
        File-Import-Subprocess
            The gateway check if there is a UploadTask with state WAITING.
                if yes, the transfer gets continued
                if no, the transfer fails with state ERROR, because there is no according UploadTask for the MinIO Upload
            The gateway has now requests the data from MinIO with state PENDING.
            The Records are created with state IN_PROGRESS
            The Upload is DONE

    So we have two major time slots where the Upload Flow is waiting:
        Between Subprocesses
        In one Subprocess

    I wanted to express that these waiting times are different from each other.
    """

    CREATED = "CREATED"
    WAITING = "WAITING"
    PENDING = "PENDING"
    PREPARED = "PREPARED"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    ERROR = "ERROR"


class UploadTypes(Enum):
    LABEL_STUDIO = "LABEL_STUDIO"
    DEFAULT = "DEFAULT"
    WORKFLOW_STORE = "WORKFLOW_STORE"
    COGNITION = "COGNITION"


class TokenizerTask(Enum):
    TYPE_DOC_BIN = "DOC_BIN"
    TYPE_TEXT = "TEXT"
    TYPE_TOKEN_STATISTICS = "TOKEN_STATISTICS"
    STATE_CREATED = "CREATED"
    STATE_IN_PROGRESS = "IN_PROGRESS"
    STATE_FINISHED = "FINISHED"
    STATE_FAILED = "FAILED"


class NotificationType(Enum):
    # TASK STATES
    IMPORT_STARTED = "IMPORT_STARTED"
    IMPORT_DONE = "IMPORT_DONE"
    IMPORT_FAILED = "IMPORT_FAILED"
    TOKEN_CREATION_STARTED = "TOKEN_CREATION_STARTED"
    TOKEN_CREATION_DONE = "TOKEN_CREATION_DONE"
    TOKEN_CREATION_FAILED = "TOKEN_CREATION_FAILED"
    EMBEDDING_CREATION_STARTED = "EMBEDDING_CREATION_STARTED"
    EMBEDDING_CREATION_DONE = "EMBEDDING_CREATION_DONE"
    EMBEDDING_CREATION_FAILED = "EMBEDDING_CREATION_FAILED"
    WEAK_SUPERVISION_TASK_STARTED = "WEAK_SUPERVISION_TASK_STARTED"
    WEAK_SUPERVISION_TASK_DONE = "WEAK_SUPERVISION_TASK_DONE"
    WEAK_SUPERVISION_TASK_FAILED = "WEAK_SUPERVISION_TASK_FAILED"
    WEAK_SUPERVISION_TASK_NO_VALID_LABELS = "WEAK_SUPERVISION_TASK_NO_VALID_LABELS"

    INFORMATION_SOURCE_STARTED = "INFORMATION_SOURCE_STARTED"
    INFORMATION_SOURCE_PREPARATION_STARTED = "INFORMATION_SOURCE_PREPARATION_STARTED"
    INFORMATION_SOURCE_COMPLETED = "INFORMATION_SOURCE_COMPLETED"
    PROJECT_DELETED = "PROJECT_DELETED"

    # INFOS
    IMPORT_SAMPLE_PROJECT = "IMPORT_SAMPLE_PROJECT"
    CONVERTING_DATA = "CONVERTING_DATA"
    COLLECTING_SESSION_DATA = "COLLECTING_SESSION_DATA"
    SESSION_INFO = "SESSION_INFO"
    UNKNOWN_DATATYPE = "UNKNOWN_DATATYPE"

    # WARNINGS
    SESSION_RECORD_AMOUNT_CHANGED = "SESSION_RECORD_AMOUNT_CHANGED"
    WRONG_USER_FOR_SESSION = "WRONG_USER_FOR_SESSION"
    DATA_SLICE_ALREADY_EXISTS = "DATA_SLICE_ALREADY_EXISTS"
    MISSING_REFERENCE_DATA = "MISSING_REFERENCE_DATA"
    EMBEDDING_CREATION_WARNING = "EMBEDDING_CREATION_WARNING"
    IMPORT_ISSUES_WARNING = "IMPORT_ISSUES_WARNING"

    # ERRORS
    INVALID_FILE_TYPE = "INVALID_FILE_TYPE"
    INVALID_PRIMARY_KEY = "INVALID_PRIMARY_KEY"
    KNOWLEDGE_BASE_ALREADY_EXISTS = "KNOWLEDGE_BASE_ALREADY_EXISTS"
    TERM_ALREADY_EXISTS = "TERM_ALREADY_EXISTS"
    FILE_TYPE_NOT_GIVEN = "FILE_TYPE_NOT_GIVEN"
    IMPORT_CONVERSION_ERROR = "IMPORT_CONVERSION_ERROR"
    UNKNOWN_PARAMETER = "UNKNOWN_PARAMETER"
    DUPLICATED_COLUMNS = "DUPLICATED_COLUMNS"
    DUPLICATED_TASK_NAMES = "DUPLICATED_TASK_NAMES"
    DUPLICATED_COMPOSITE_KEY = "DUPLICATED_COMPOSITE_KEY"
    DIFFERENTIAL_ATTRIBUTES = "DIFFERENTIAL_ATTRIBUTES"
    NON_EXISTENT_TARGET_ATTRIBUTE = "NON_EXISTENT_TARGET_ATTRIBUTE"
    UPLOAD_CONVERSION_FAILED = "UPLOAD_CONVERSION_FAILED"
    INFORMATION_SOURCE_FAILED = "INFORMATION_SOURCE_FAILED"
    INFORMATION_SOURCE_CANT_FIND_EMBEDDING = "INFORMATION_SOURCE_CANT_FIND_EMBEDDING"
    INFORMATION_SOURCE_S3_EMBEDDING_MISSING = "INFORMATION_SOURCE_S3_EMBEDDING_MISSING"
    INFORMATION_SOURCE_S3_DOCBIN_MISSING = "INFORMATION_SOURCE_S3_DOCBIN_MISSING"
    NEW_ROWS_EXCEED_MAXIMUM_LIMIT = "NEW_ROWS_EXCEED_MAXIMUM_LIMIT"
    TOTAL_ROWS_EXCEED_MAXIMUM_LIMIT = "TOTAL_ROWS_EXCEED_MAXIMUM_LIMIT"
    COLS_EXCEED_MAXIMUM_LIMIT = "COLS_EXCEED_MAXIMUM_LIMIT"
    COL_EXCEED_MAXIMUM_LIMIT = "COL_EXCEED_MAXIMUM_LIMIT"
    DATA_SLICE_CREATION_FAILED = "DATA_SLICE_CREATION_FAILED"
    DATA_SLICE_UPDATE_FAILED = "DATA_SLICE_UPDATE_FAILED"
    BAD_PASSWORD_DURING_IMPORT = "BAD_PASSWORD_DURING_IMPORT"
    RECREATION_OF_EMBEDDINGS_ERROR = "RECREATION_OF_EMBEDDINGS_ERROR"

    # CUSTOM
    CUSTOM = "CUSTOM"


class NotificationState(Enum):
    INITIAL = "INITIAL"
    NOT_INITIAL = "NOT_INITIAL"


class Pages(Enum):
    OVERVIEW = "overview"
    DATA = "data"
    LABELING = "labeling"
    INFORMATION_SOURCES = "heuristics"
    KNOWLEDGE_BASE = "lookup-lists"
    SETTINGS = "settings"


class DOCS(Enum):
    UPLOADING_DATA = "https://docs.kern.ai/refinery/project-creation-and-data-upload"
    KNOWLEDGE_BASE = "https://docs.kern.ai/refinery/heuristics#labeling-functions"
    WORKFLOW = "https://docs.kern.ai/refinery/manual-labeling#labeling-workflow"
    CREATING_PROJECTS = "https://docs.kern.ai/refinery/project-creation-and-data-upload#project-creation-workflow"
    WEAK_SUPERVISION = "https://docs.kern.ai/refinery/weak-supervision"
    CREATE_EMBEDDINGS = "https://docs.kern.ai/refinery/embedding-integration"
    INFORMATION_SOURCES = "https://docs.kern.ai/refinery/heuristics#labeling-functions"
    DATA_BROWSER = "https://docs.kern.ai/refinery/data-management"


class SliceTypes(Enum):
    STATIC_DEFAULT = "STATIC_DEFAULT"
    STATIC_OUTLIER = "STATIC_OUTLIER"
    DYNAMIC_DEFAULT = "DYNAMIC_DEFAULT"


class InterAnnotatorConstants(Enum):
    ID_GOLD_USER = "GOLD_STAR"
    ID_NULL_USER = "NULL_USER"


class EmbeddingState(Enum):
    INITIALIZING = "INITIALIZING"
    WAITING = "WAITING"
    ENCODING = "ENCODING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class AttributeState(Enum):
    UPLOADED = "UPLOADED"
    AUTOMATICALLY_CREATED = "AUTOMATICALLY_CREATED"
    INITIAL = "INITIAL"
    RUNNING = "RUNNING"
    USABLE = "USABLE"
    FAILED = "FAILED"


class AttributeVisibility(Enum):
    HIDE = "HIDE"  # hide attributes on all pages
    HIDE_ON_LABELING_PAGE = "HIDE_ON_LABELING_PAGE"  # hide attributes on labeling page and data browser page
    HIDE_ON_DATA_BROWSER = "HIDE_ON_DATA_BROWSER"  # hide attributes on data browser page but not on labeling page
    DO_NOT_HIDE = "DO_NOT_HIDE"  # do not hide attributes on any page


class RecordExportFormats(Enum):
    DEFAULT = "DEFAULT"
    LABEL_STUDIO = "LABEL_STUDIO"


class RecordImportFileTypes(Enum):
    JSON = "JSON"
    CSV = "CSV"
    XLSX = "XLSX"


class RecordExportFileTypes(Enum):
    JSON = "JSON"
    CSV = "CSV"
    XLSX = "XLSX"


class RecordExportAmountTypes(Enum):
    ALL = "ALL"
    SESSION = "SESSION"
    SLICE = "SLICE"


class RecordImportMappingValues(Enum):
    ATTRIBUTE_SPECIFIC = "ATTRIBUTE_SPECIFIC"
    IGNORE = "IGNORE"
    UNKNOWN = "UNKNOWN"


class TokenExpireAtValues(Enum):
    ONE_MONTH = "ONE_MONTH"
    THREE_MONTHS = "THREE_MONTHS"
    NEVER = "NEVER"


class TokenScope(Enum):
    READ = "READ"
    READ_WRITE = "READ_WRITE"


class TokenizationTaskTypes(Enum):
    ATTRIBUTE = "ATTRIBUTE"
    PROJECT = "PROJECT"


class RecordTokenizationScope(Enum):
    PROJECT = "PROJECT"
    ATTRIBUTE = "ATTRIBUTE"


class GatesIntegrationStatus(Enum):
    READY = "READY"
    NOT_READY = "NOT_READY"
    UPDATING = "UPDATING"


class AdminMessageLevel(Enum):
    WARNING = "WARNING"
    INFO = "INFO"


class TaskType(Enum):
    TOKENIZATION = "tokenization"
    ATTRIBUTE_CALCULATION = "attribute_calculation"
    EMBEDDING = "embedding"
    INFORMATION_SOURCE = "information_source"
    UPLOAD_TASK = "upload"
    WEAK_SUPERVISION = "weak_supervision"
    PARSE_MARKDOWN_FILE = "PARSE_MARKDOWN_FILE"
    PARSE_COGNITION_TMP_FILE = "PARSE_COGNITION_TMP_FILE"
    TASK_QUEUE = "task_queue"
    TASK_QUEUE_ACTION = "task_queue_action"
    RUN_COGNITION_MACRO = "RUN_COGNITION_MACRO"


class TaskQueueAction(Enum):
    CREATE_OUTLIER_SLICE = "CREATE_OUTLIER_SLICE"
    START_GATES = "START_GATES"
    SEND_WEBSOCKET = "SEND_WEBSOCKET"
    FINISH_COGNITION_SETUP = "FINISH_COGNITION_SETUP"
    RUN_WEAK_SUPERVISION = "RUN_WEAK_SUPERVISION"


class AgreementType(Enum):
    EMBEDDING = "EMBEDDING"


class EmbeddingPlatform(Enum):
    PYTHON = "python"
    HUGGINGFACE = "huggingface"
    COHERE = "cohere"
    OPENAI = "openai"
    AZURE = "azure"


class SampleProjectType(Enum):
    CLICKBAIT_INITIAL = "Clickbait - initial"
    CLICKBAIT = "Clickbait"
    AG_NEWS_INITIAL = "AG News - initial"
    AG_NEWS = "AG News"
    CONVERSATIONAL_AI_INITIAL = "Conversational AI - initial"
    CONVERSATIONAL_AI = "Conversational AI"
    DEV_GLOBAL_GUARD_REFERENCES = "Global Guard [References]"
    DEV_GLOBAL_GUARD_QUESTIONS = "Global Guard [Questions]"


class StrategyStepType(Enum):
    RETRIEVAL = "RETRIEVAL"
    # unused option that was never fully implemented
    # RELEVANCE = "RELEVANCE"
    NONE = "NONE"
    PYTHON = "PYTHON"
    LLM = "LLM"
    SELECTION = "SELECTION"
    # now more like a common llm node but changing the enum value would break the existing data
    QUERY_REPHRASING = "QUERY_REPHRASING"
    # INFO: Websearch strategy deactivated until compliance investigation is finished
    # WEBSEARCH = "WEBSEARCH" # done in exec env to ensure security
    TRUNCATE_CONTEXT = "TRUNCATE_CONTEXT"
    HEADER = "HEADER"
    # INFO: done in exec env to prevent installing sklearn in gateway
    TMP_DOC_RETRIEVAL = "TMP_DOC_RETRIEVAL"
    # INFO: will replace retrieval in the future, direct access to neural search without gates
    NEURAL_SEARCH = "NEURAL_SEARCH"

    def get_description(self):
        return STEP_DESCRIPTIONS.get(self, "No description available")

    def get_when_to_use(self):
        return STEP_WHEN_TO_USE.get(self, "No description available")

    def get_progress_text(self):
        return STEP_PROGRESS_TEXTS.get(self, "No progress text available")


STEP_DESCRIPTIONS = {
    StrategyStepType.RETRIEVAL: "Fetch facts from a DB",
    StrategyStepType.NEURAL_SEARCH: "Fetch facts from an embedding",
    # StrategyStepType.RELEVANCE: "Classify retrieved facts",
    StrategyStepType.NONE: "Dummy step",
    StrategyStepType.PYTHON: "Custom python function",
    StrategyStepType.LLM: "Answer with LLM",
    StrategyStepType.SELECTION: "Select data",
    StrategyStepType.QUERY_REPHRASING: "Transform with LLM",
    # INFO: Websearch strategy deactivated until compliance investigation is finished
    # StrategyStepType.WEBSEARCH: "Search the web",
    StrategyStepType.TRUNCATE_CONTEXT: "Truncate context",
    StrategyStepType.HEADER: "Writing header",
    StrategyStepType.TMP_DOC_RETRIEVAL: "Temporary document retrieval",
}

STEP_WHEN_TO_USE = {
    StrategyStepType.RETRIEVAL: "When you want to retrieve facts from a database",
    StrategyStepType.NEURAL_SEARCH: "When you want to fetch facts based on an embedding",
    # StrategyStepType.RELEVANCE: "When you want to classify retrieved facts",
    StrategyStepType.PYTHON: "When you want to run a custom python function",
    StrategyStepType.LLM: "When you want to give an actual answer to the question",
    StrategyStepType.NONE: "Dummy step",
    StrategyStepType.SELECTION: "When you want to select data",
    StrategyStepType.QUERY_REPHRASING: "When you want to manipulate or extend your attributes with a LLM",
    # INFO: Websearch strategy deactivated until compliance investigation is finished
    # StrategyStepType.WEBSEARCH: "When you want to search the web",
    StrategyStepType.TRUNCATE_CONTEXT: "When you want to truncate context",
    StrategyStepType.HEADER: "When you want to set a header based on the conversation",
    StrategyStepType.TMP_DOC_RETRIEVAL: "When you want to retrieve results from conversation specific documents",
}

STEP_PROGRESS_TEXTS = {
    StrategyStepType.RETRIEVAL: "Retrieving facts",
    StrategyStepType.NEURAL_SEARCH: "Retrieving facts",
    # StrategyStepType.RELEVANCE: "Classifying facts",
    StrategyStepType.NONE: "Dummy step",
    StrategyStepType.PYTHON: "Running custom python function",
    StrategyStepType.LLM: "Running LLM",
    StrategyStepType.SELECTION: "Selecting data",
    StrategyStepType.QUERY_REPHRASING: "Rephrasing query",
    # INFO: Websearch strategy deactivated until compliance investigation is finished
    # StrategyStepType.WEBSEARCH: "Searching the web",
    StrategyStepType.TRUNCATE_CONTEXT: "Truncating context",
    StrategyStepType.HEADER: "Headline generation",
    StrategyStepType.TMP_DOC_RETRIEVAL: "Retrieving facts from conversation specific documents",
}

STEP_ERRORS = {
    StrategyStepType.HEADER.value: "Header must come after field answer is set in the record_dict.",
}


class PipelineStep(Enum):
    INCOMING_QUESTION = "INCOMING_QUESTION"
    INCOMING_QUESTION_TRY = "INCOMING_QUESTION_TRY"
    QUESTION_ENRICHMENT = "QUESTION_ENRICHMENT"
    ROUTE_STRATEGY = "ROUTE_STRATEGY"
    STRATEGY_STEP = "STRATEGY_STEP"
    ASSISTANT_RESPONSE = "ASSISTANT_RESPONSE"


class PipelineStepState(Enum):
    STARTED = "STARTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class MarkdownFileCategoryOrigin(Enum):
    PDF = "PDF"
    WEB = "WEB"
    SPREADSHEET = "SPREADSHEET"


class RefinerySynchronizationTaskState(Enum):
    CREATED = "CREATED"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class RefinerySynchronizationIntervalOption(Enum):
    NEVER = "NEVER"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class LLMProvider(Enum):
    OPENAI = "Open AI"
    OPEN_SOURCE = "Open-Source"
    AZURE = "Azure"


class OpenAIClientType(Enum):
    OPEN_AI = "OPEN_AI"
    AZURE = "AZURE"


class CognitionMarkdownFileState(Enum):
    QUEUE = "QUEUE"
    EXTRACTING = "EXTRACTING"
    TOKENIZING = "TOKENIZING"
    SPLITTING = "SPLITTING"
    TRANSFORMING = "TRANSFORMING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class CognitionInterfaceType(Enum):
    CHAT = "CHAT"
    COMPARE = "COMPARE"
    ENRICH = "ENRICH"


class EmitType(Enum):
    ANSWER = "ANSWER"
    RETRIEVAL_RESULTS = "RETRIEVAL_RESULTS"
    FOLLOW_UPS = "FOLLOW_UPS"
    SELECTION = "SELECTION"
    QUERY_REPHRASING = "QUERY_REPHRASING"


class CognitionProjectState(Enum):
    CREATED = "CREATED"
    WIZARD_RUNNING = "WIZARD_RUNNING"
    DEVELOPMENT = "DEVELOPMENT"
    PRODUCTION = "PRODUCTION"


class StrategyComplexity(Enum):
    SIMPLE = "SIMPLE"
    REGULAR = "REGULAR"
    COMPLEX = "COMPLEX"


class ConsumptionLogState(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"


class CognitionConfigKey(Enum):
    STRATEGY_COMPLEXITY_THRESHOLD = "STRATEGY_COMPLEXITY_THRESHOLD"
    STRATEGY_STEP_WEIGHTS = "STRATEGY_STEP_WEIGHTS"


# note this is only for websocket interaction between exec env and gateway
# none of these can/is allowed to interact with the database or anything other than the websocket!
# means if you want a live update and set it as answer this needs to be done in the exec env code record_dict change
class AllowedExecEnvMessageTypes(Enum):
    CHUNK = "CHUNK"  # sends a chunk to the ui - same as llm step type
    SET_UI_MESSAGE = "SET_UI_MESSAGE"  # replaces answer in the ui
    CLOSE = "CLOSE"  # closes the websocket - shouldn't be sent by hand!


def try_parse_enum_value(string: str, enumType: Enum, raise_me: bool = True) -> Any:
    try:
        parsed = enumType[string.upper()]
    except KeyError:
        if raise_me:
            raise ValueError(f"Invalid value {string} for enum {enumType}")
        return
    return parsed


# this is only a method check.
# Some endpoints were implemented as POST requests to allow a body even though they are not creating anything
# Example comment/all-comments. For these i added a dependency extend_state_get_like with sets a state variable to use as indicator
class AdminLogLevel(Enum):
    DONT_LOG = "DONT_LOG"  # nothing is logged
    NO_GET = "NO_GET"  # everything but method GET is logged
    ALL = "ALL"  # everything is logged

    def log_me(self, method: str) -> bool:
        if self == AdminLogLevel.DONT_LOG:
            return False
        if self == AdminLogLevel.NO_GET and method == "GET":
            return False
        return True


# currently only one option, but could be extended in the future
class MacroType(Enum):
    # macro is meant to be run on a (or n) documents
    DOCUMENT_MESSAGE_QUEUE = "DOCUMENT_MESSAGE_QUEUE"
    FOLDER_MESSAGE_QUEUE = "FOLDER_MESSAGE_QUEUE"


# currently only one option, but could be extended in the future
class MacroNodeContentType(Enum):
    # add a new question to the conversation
    CONVERSATION_QUESTION = "CONVERSATION_QUESTION"


# currently only one option, but could be extended in the future
class MacroEdgeConditionType(Enum):
    # all edges from a given node are collected and an llm decides which to chose based on the name of the edge
    LLM_SELECTION = "LLM_SELECTION"


class MacroScope(Enum):
    ADMIN = "ADMIN"
    ORGANIZATION = "ORGANIZATION"
    PROJECT = "PROJECT"


class MacroState(Enum):
    DEVELOPMENT = "DEVELOPMENT"
    PRODUCTION = "PRODUCTION"


class MacroExecutionState(Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class MacroExecutionLinkAction(Enum):
    CREATE = "CREATE"
    DELETE = "DELETE"
    UPDATE = "UPDATE"


class AdminMacrosDisplay(Enum):
    DONT_SHOW = "DONT_SHOW"
    FOR_ADMINS = "FOR_ADMINS"
    FOR_ENGINEERS = "FOR_ENGINEERS"
    FOR_ALL = "FOR_ALL"


class CustomerButtonType(Enum):

    DATA_MAPPER = "DATA_MAPPER"
    # sends data to the data mapper, needs to ensure the request has the key included!

    # ______________________________
    # extended on demand over time


class CustomerButtonLocation(Enum):
    COGNITION_MACRO_RESULTS_TABLE = "COGNITION_MACRO_RESULTS_TABLE"  # url /macros/<macro_id> # only visible if meta data display is active

    # extended on demand over time
