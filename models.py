import uuid

from .enums import (
    AdminLogLevel,
    AdminMessageLevel,
    AttributeState,
    AttributeVisibility,
    CascadeBehaviour,
    CognitionProjectState,
    FileCachingState,
    Notification as NotificationEnums,
    NotificationState,
    PayloadState,
    PipelineVersionType,
    SliceTypes,
    StrategyComplexity,
    Tablenames,
    TokenLimit,
    TokenScope,
    TokenSubject,
    UploadStates,
    UserRoles,
    CognitionMarkdownFileState,
)
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    LargeBinary,
    String,
    sql,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    backref,
    relationship,
)
from sqlalchemy.types import ARRAY


Base = declarative_base()
metadata = Base.metadata


# 1:N
def parent_to_child_relationship(
    this_table: Tablenames,
    other_table: Tablenames,
    cascase_behaviour: CascadeBehaviour = CascadeBehaviour.KEEP_PARENT_ON_CHILD_DELETION,
    order_by=None,
):
    if order_by is not None:
        if isinstance(order_by, list):
            tmp = "["
            for e in order_by:
                tmp += f"{other_table.snake_case_to_pascal_case()}.{e}, "
            order_by = tmp[:-2] + "]"
        else:
            order_by = f"{other_table.snake_case_to_pascal_case()}.{order_by}"
    else:
        order_by = False

    if cascase_behaviour == CascadeBehaviour.KEEP_PARENT_ON_CHILD_DELETION:
        return relationship(
            other_table.snake_case_to_pascal_case(),
            backref=backref(
                this_table.snake_case_to_camel_case(),
            ),
            cascade="delete,all",
            order_by=order_by,
        )
    elif cascase_behaviour == CascadeBehaviour.DELETE_BOTH_IF_EITHER_IS_DELETED:
        return relationship(
            other_table.snake_case_to_pascal_case(),
            backref=backref(
                this_table.snake_case_to_camel_case(),
                cascade="delete,all",
            ),
            order_by=order_by,
        )


# -------------------- INFO_ ----------------------
#
# Table column order convention:
# note that this is a guideline and not a strict rule
# e.g. context based columns should generally be grouped together
#
# 0. __tablename__ & __table_args__ if needed
# 1. id (primary key/s)
# 2. foreign keys
#   2.1 sort from broader scope to more specific (e.g. org_id > proj_id > user_id)
# 3. other columns
#   3.1 sort from broader scope to more specific (e.g. name > description > some flag)
# 4. columns added after initial creation
#
# -------------------- GLOBAL_ --------------------
class AppVersion(Base):
    __tablename__ = Tablenames.APP_VERSION.value
    service = Column(String, primary_key=True)
    installed_version = Column(String)  # local/installed Tag
    remote_version = Column(String)  # latest GitHub Tag
    last_checked = Column(DateTime)


class CommentData(Base):
    __tablename__ = Tablenames.COMMENT_DATA.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    # no foreign key since its a multi field
    xfkey = Column(UUID(as_uuid=True), index=True)
    # of type CommentCategory e.g. USER
    xftype = Column(String, index=True)
    # key for e.g. multiple comments on a single user
    order_key = Column(Integer, autoincrement=True)
    comment = Column(String)
    is_markdown = Column(Boolean, default=False)
    is_private = Column(Boolean, default=False)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())


class Organization(Base):
    __tablename__ = Tablenames.ORGANIZATION.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, unique=True)
    # when did the company start using the app (trail time start)
    started_at = Column(DateTime)
    # database entry
    is_paying = Column(Boolean, default=False)
    created_at = Column(DateTime, default=sql.func.now())
    projects = parent_to_child_relationship(
        Tablenames.ORGANIZATION,
        Tablenames.PROJECT,
    )
    users = parent_to_child_relationship(
        Tablenames.ORGANIZATION,
        Tablenames.USER,
    )
    max_rows = Column(Integer, default=50000)
    max_cols = Column(Integer, default=25)
    max_char_count = Column(Integer, default=100000)

    # designed as opt out to ensure "forgotten" doesn't result in issues
    log_admin_requests = Column(String, default=AdminLogLevel.NO_GET.value)
    conversation_lifespan_days = Column(Integer)
    file_lifespan_days = Column(Integer, default=14)
    token_limit = Column(
        JSON,
        default={
            TokenLimit.FILE_UPLOAD_LIMIT.lowercase(): 50,
            TokenLimit.FILE_UPLOAD_INTERVAL.lowercase(): 3600,
        },
    )  # per hour


class User(Base):
    __tablename__ = Tablenames.USER.value
    id = Column(UUID(as_uuid=True), primary_key=True)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    role = Column(String, default=UserRoles.ENGINEER.value)  # enum UserRoles
    language_display = Column(String, default="en")
    notifications = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.NOTIFICATION,
    )
    information_sources = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.INFORMATION_SOURCE,
    )
    embeddings = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.EMBEDDING,
    )
    user_queries = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.USER_SESSIONS,
    )
    user_record_label_associations = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.RECORD_LABEL_ASSOCIATION,
    )
    weak_supervision_runs = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.WEAK_SUPERVISION_TASK,
        order_by="created_at.desc()",
    )
    projects = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.PROJECT,
        order_by="created_at.desc()",
    )
    comments = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.COMMENT_DATA,
        order_by="created_at.desc()",
    )
    agreements = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.AGREEMENT,
        order_by="created_at.desc()",
    )
    last_interaction = Column(DateTime)
    email = Column(String, unique=True)
    verified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=sql.func.now())
    metadata_public = Column(JSON)
    sso_provider = Column(String)
    use_new_cognition_ui = Column(Boolean, default=True)
    auto_logout_minutes = Column(Integer)
    messages_created_this_month = Column(BigInteger, default=0)
    one_drive_path = Column(String)


class Team(Base):
    __tablename__ = Tablenames.TEAM.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    name = Column(String)
    description = Column(String)


class TeamMember(Base):
    __tablename__ = Tablenames.TEAM_MEMBER.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    team_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.TEAM.value}.id", ondelete="CASCADE"),
        index=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())


class TeamResource(Base):
    __tablename__ = Tablenames.TEAM_RESOURCE.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    team_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.TEAM.value}.id", ondelete="CASCADE"),
        index=True,
    )
    resource_id = Column(
        UUID(as_uuid=True),
        index=True,
    )
    resource_type = Column(String)  # of type enums.ResourceType.*.value
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())


class LabelingAccessLink(Base):
    __tablename__ = Tablenames.LABELING_ACCESS_LINK.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    # router link without domain e.g. /app/projects/399d9a46-1f7f-4781-aafb-2af0f4a017e5/labeling/81c74109-0f6d-491d-ac33-6e83f6c011e5?pos=1&type=SESSION
    link = Column(String)

    # as own ids not a combined one to leverage cascade behaviour
    data_slice_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.DATA_SLICE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    heuristic_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.INFORMATION_SOURCE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    link_type = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    is_locked = Column(Boolean, default=False)
    # corresponding data last changed at (e.g. if a data slice was updated or the heuristic was updated)
    changed_at = Column(DateTime, default=sql.func.now())


class UserSessions(Base):
    __tablename__ = Tablenames.USER_SESSIONS.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    id_sql_statement = Column(String)
    count_sql_statement = Column(String)
    last_count = Column(Integer)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    session_record_ids = Column(JSON)
    random_seed = Column(Float)
    # to prevent an in use session from being deleted
    temp_session = Column(Boolean, default=True)


class Notification(Base):
    __tablename__ = Tablenames.NOTIFICATION.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # maybe remove delete cascade to prevent notifications from vanishing if e.g. a project is removed?
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    type = Column(String)  # of type enums.NotificationType.*.value
    level = Column(String, default=NotificationEnums.INFO.value)
    message = Column(String)
    important = Column(Boolean)
    state = Column(String, default=NotificationState.INITIAL.value)
    created_at = Column(DateTime, default=sql.func.now())


class UploadTask(Base):
    __tablename__ = Tablenames.UPLOAD_TASK.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    state = Column(String, default=UploadStates.CREATED.value)
    progress = Column(Float, default=0.0)
    started_at = Column(DateTime, default=sql.func.now())
    finished_at = Column(DateTime)
    file_name = Column(String)
    file_type = Column(String)
    file_import_options = Column(String)
    upload_type = Column(String)
    file_additional_info = Column(String)
    mappings = Column(String)
    key = Column(LargeBinary)


class Agreement(Base):
    __tablename__ = Tablenames.AGREEMENT.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    # no foreign key since its a multi field
    xfkey = Column(UUID(as_uuid=True), index=True, nullable=True)
    # of type AgreementCategory e.g. EMBEDDING, INFORMATION_SOURCE
    xftype = Column(String, index=True, nullable=True)
    terms_text = Column(String)
    terms_accepted = Column(Boolean)
    created_at = Column(DateTime, default=sql.func.now())


# -------------------- PROJECT_ --------------------
class Project(Base):
    __tablename__ = Tablenames.PROJECT.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    description = Column(String)
    tokenizer = Column(String)
    tokenizer_blank = Column(String)
    status = Column(String)  # e.g. INIT_UPLOAD, INIT_COMPLETE
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )

    records = parent_to_child_relationship(
        Tablenames.PROJECT,
        Tablenames.RECORD,
    )
    attributes = parent_to_child_relationship(
        Tablenames.PROJECT,
        Tablenames.ATTRIBUTE,
        order_by="relative_position",
    )
    # access to labeling tasks without connection to attribute relation is possible
    labeling_tasks = parent_to_child_relationship(
        Tablenames.PROJECT,
        Tablenames.LABELING_TASK,
    )
    embeddings = parent_to_child_relationship(
        Tablenames.PROJECT,
        Tablenames.EMBEDDING,
    )
    information_sources = parent_to_child_relationship(
        Tablenames.PROJECT,
        Tablenames.INFORMATION_SOURCE,
        order_by=["created_at.desc()", "name.asc()", "id.desc()"],
    )
    knowledge_bases = parent_to_child_relationship(
        Tablenames.PROJECT,
        Tablenames.KNOWLEDGE_BASE,
    )
    upload_tasks = parent_to_child_relationship(
        Tablenames.PROJECT,
        Tablenames.UPLOAD_TASK,
    )
    data_slices = parent_to_child_relationship(
        Tablenames.PROJECT,
        Tablenames.DATA_SLICE,
    )


class Attribute(Base):
    __tablename__ = Tablenames.ATTRIBUTE.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    data_type = Column(String)
    is_primary_key = Column(Boolean, default=False)
    relative_position = Column(Integer)
    user_created = Column(Boolean, default=False)
    source_code = Column(String)
    state = Column(String, default=AttributeState.UPLOADED.value)
    logs = Column(ARRAY(String))
    visibility = Column(String, default=AttributeVisibility.DO_NOT_HIDE.value)
    started_at = Column(DateTime, default=sql.func.now())
    finished_at = Column(DateTime)
    progress = Column(Float)
    additional_config = Column(JSON, comment="used when data_type == LLM_RESPONSE")

    embeddings = parent_to_child_relationship(
        Tablenames.ATTRIBUTE,
        Tablenames.EMBEDDING,
    )

    labeling_tasks = parent_to_child_relationship(
        Tablenames.ATTRIBUTE,
        Tablenames.LABELING_TASK,
    )


class LabelingTask(Base):
    __tablename__ = Tablenames.LABELING_TASK.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    # attribute_id does not have to be set; if it is not set,
    # then the labeling is bound to the whole record in general, not a single attribute.
    # this can also be seen in the task_target
    attribute_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ATTRIBUTE.value}.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    task_target = Column(String)  # ON_ATTRIBUTE, ON_WHOLE_RECORD
    task_type = Column(String)  # CLASSIFICATION, EXTRACTION

    information_sources = parent_to_child_relationship(
        Tablenames.LABELING_TASK,
        Tablenames.INFORMATION_SOURCE,
    )
    labels = parent_to_child_relationship(
        Tablenames.LABELING_TASK, Tablenames.LABELING_TASK_LABEL, order_by="created_at"
    )


class LabelingTaskLabel(Base):
    __tablename__ = Tablenames.LABELING_TASK_LABEL.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    name = Column(String)
    labeling_task_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.LABELING_TASK.value}.id", ondelete="CASCADE"),
        index=True,
    )
    color = Column(String)
    hotkey = Column(String)
    record_label_associations = parent_to_child_relationship(
        Tablenames.LABELING_TASK_LABEL,
        Tablenames.RECORD_LABEL_ASSOCIATION,
    )
    information_source_statistics = parent_to_child_relationship(
        Tablenames.LABELING_TASK_LABEL,
        Tablenames.INFORMATION_SOURCE_STATISTICS,
    )


# -------------------- DATA_SLICE_ --------------------
class DataSlice(Base):
    __tablename__ = Tablenames.DATA_SLICE.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    filter_data = Column(JSON)
    filter_raw = Column(JSON)
    static = Column(Boolean, default=False)
    count = Column(Integer)
    count_sql = Column(String)
    slice_type = Column(String, default=SliceTypes.DYNAMIC_DEFAULT.value)
    info = Column(JSON)


class DataSliceRecordAssociation(Base):
    __tablename__ = Tablenames.DATA_SLICE_RECORD_ASSOCIATION.value
    data_slice_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.DATA_SLICE.value}.id", ondelete="CASCADE"),
        primary_key=True,
    )
    record_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.RECORD.value}.id", ondelete="CASCADE"),
        primary_key=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    outlier_score = Column(Float)


# -------------------- RECORD_ --------------------
class Record(Base):
    __tablename__ = Tablenames.RECORD.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    data = Column(JSON)
    category = Column(String)  # e.g. SCALE or TEST
    created_at = Column(DateTime, default=sql.func.now(), index=True)

    record_label_associations = parent_to_child_relationship(
        Tablenames.RECORD,
        Tablenames.RECORD_LABEL_ASSOCIATION,
    )
    tensors = parent_to_child_relationship(
        Tablenames.RECORD,
        Tablenames.EMBEDDING_TENSOR,
    )


class RecordTokenized(Base):
    # this is a byte dump for the docbins of spacy! not human readable
    __tablename__ = Tablenames.RECORD_TOKENIZED.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    record_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.RECORD.value}.id", ondelete="CASCADE"),
        index=True,
    )
    bytes = Column(LargeBinary)
    columns = Column(ARRAY(String))


class RecordTokenizationTask(Base):
    __tablename__ = Tablenames.RECORD_TOKENIZATION_TASK.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    type = Column(String, default="DOC_BINS")
    scope = Column(String, default="PROJECT")  # e.g. PROJECT, ATTRIBUTE
    attribute_name = Column(String)
    state = Column(String, default=UploadStates.CREATED.value)
    progress = Column(Float, default=0.0)
    workload = Column(Integer)
    started_at = Column(DateTime, default=sql.func.now())
    finished_at = Column(DateTime)


class RecordLabelAssociation(Base):
    __tablename__ = Tablenames.RECORD_LABEL_ASSOCIATION.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    record_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.RECORD.value}.id", ondelete="CASCADE"),
        index=True,
    )
    labeling_task_label_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.LABELING_TASK_LABEL.value}.id", ondelete="CASCADE"),
        index=True,
    )
    source_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.INFORMATION_SOURCE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    weak_supervision_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.WEAK_SUPERVISION_TASK.value}.id", ondelete="CASCADE"),
        index=True,
    )
    # e.g. MANUAL, INFORMATION_SOURCE, WEAK_SUPERVISION
    source_type = Column(String)
    return_type = Column(String)  # e.g. YIELD, RETURN
    confidence = Column(Float)

    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    # gold_star are created labels for conflict resolution
    is_gold_star = Column(Boolean)
    # combines gold start etc
    is_valid_manual_label = Column(Boolean, index=True)

    tokens = parent_to_child_relationship(
        Tablenames.RECORD_LABEL_ASSOCIATION,
        Tablenames.RECORD_LABEL_ASSOCIATION_TOKEN,
        CascadeBehaviour.DELETE_BOTH_IF_EITHER_IS_DELETED,
        order_by="token_index.asc()",
    )


class RecordLabelAssociationToken(Base):
    __tablename__ = Tablenames.RECORD_LABEL_ASSOCIATION_TOKEN.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    record_label_association_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"{Tablenames.RECORD_LABEL_ASSOCIATION.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    token_index = Column(Integer)
    is_beginning_token = Column(Boolean)


class RecordAttributeTokenStatistics(Base):
    __tablename__ = Tablenames.RECORD_ATTRIBUTE_TOKEN_STATISTICS.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    record_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.RECORD.value}.id", ondelete="CASCADE"),
        index=True,
    )
    attribute_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ATTRIBUTE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    num_token = Column(Integer)


class Embedding(Base):
    __tablename__ = Tablenames.EMBEDDING.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    attribute_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ATTRIBUTE.value}.id", ondelete="CASCADE"),
    )
    name = Column(String)
    custom = Column(
        Boolean
    )  # custom = provided by user at transfer, not custom = calculated via embedding-service
    type = Column(String)  # find allowed expressions in enums.EmbeddingType
    state = Column(String)  # set by embedding service
    similarity_threshold = Column(Float)  # set by neural search
    started_at = Column(DateTime, default=sql.func.now())
    finished_at = Column(DateTime)
    # attributes that can be used to filter in qdrant
    filter_attributes = Column(ARRAY(String))
    # due to security reasons will not be exported or imported
    api_token = Column(String)
    model = Column(String)
    platform = Column(String)
    tensors = parent_to_child_relationship(
        Tablenames.EMBEDDING,
        Tablenames.EMBEDDING_TENSOR,
    )
    additional_data = Column(JSON)

    # threshold indicates when the embedding should be completely recalculated
    delta_full_recalculation_threshold = Column(Float, default=0.5)
    # holds the current number of records that were caluclated with the previous PCA if new records + current delta > threshold we recreate completely
    # note that this number can be higher than expected because of updated records being recalculated as well
    # meaning in theory if someone updates the same record over and over again at some point the full recalculation will be triggered
    current_delta_record_count = Column(Integer, default=0)


class EmbeddingTensor(Base):
    __tablename__ = Tablenames.EMBEDDING_TENSOR.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    record_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.RECORD.value}.id", ondelete="CASCADE"),
        index=True,
    )
    embedding_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.EMBEDDING.value}.id", ondelete="CASCADE"),
        index=True,
    )
    sub_key = Column(Integer)
    data = Column(JSON)


# -------------------- INFORMATION_INTEGRATION_ --------------------
class InformationSource(Base):  # renamed from LabelFunction
    __tablename__ = Tablenames.INFORMATION_SOURCE.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    labeling_task_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.LABELING_TASK.value}.id", ondelete="CASCADE"),
        index=True,
    )
    # e.g. LABELING_FUNCTION, ACTIVE_LEARNING_MODEL, API, ...
    type = Column(String)
    return_type = Column(String)  # e.g. RETURN, YIELD
    name = Column(String)
    description = Column(String)
    source_code = Column(String)
    is_selected = Column(Boolean, default=False)
    version = Column(Integer, default=1)

    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )

    source_statistics = parent_to_child_relationship(
        Tablenames.INFORMATION_SOURCE,
        Tablenames.INFORMATION_SOURCE_STATISTICS,
    )
    payloads = parent_to_child_relationship(
        Tablenames.INFORMATION_SOURCE,
        Tablenames.INFORMATION_SOURCE_PAYLOAD,
        order_by="iteration.desc()",
    )
    record_label_associations = parent_to_child_relationship(
        Tablenames.INFORMATION_SOURCE,
        Tablenames.RECORD_LABEL_ASSOCIATION,
    )


class InformationSourceStatistics(Base):
    __tablename__ = Tablenames.INFORMATION_SOURCE_STATISTICS.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    source_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{InformationSource.__tablename__}.id", ondelete="CASCADE"),
        index=True,
    )
    labeling_task_label_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.LABELING_TASK_LABEL.value}.id", ondelete="CASCADE"),
        index=True,
    )
    true_positives = Column(Integer)
    false_positives = Column(Integer)
    false_negatives = Column(Integer)
    record_coverage = Column(Integer)
    total_hits = Column(Integer)
    source_conflicts = Column(Integer)
    source_overlaps = Column(Integer)


class InformationSourceStatisticsExclusion(Base):
    __tablename__ = Tablenames.INFORMATION_SOURCE_STATISTICS_EXCLUSION.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    record_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"{Tablenames.RECORD.value}.id",
            ondelete="CASCADE",
        ),
        index=True,
    )
    source_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.INFORMATION_SOURCE.value}.id", ondelete="CASCADE"),
        index=True,
    )


class InformationSourcePayload(Base):
    __tablename__ = Tablenames.INFORMATION_SOURCE_PAYLOAD.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    source_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.INFORMATION_SOURCE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    state = Column(
        String, default=PayloadState.CREATED.value
    )  # e.g. CREATED, FINISHED, FAILED
    progress = Column(Float, default=0.0)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    finished_at = Column(DateTime)
    iteration = Column(Integer)
    source_code = Column(String)
    input_data = Column(JSON)
    output_data = Column(JSON)
    logs = Column(ARRAY(String))


# -------------------- WEAK_SUPERVISION_ ------------------
class WeakSupervisionTask(Base):
    __tablename__ = Tablenames.WEAK_SUPERVISION_TASK.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    state = Column(String)  # e.g. CREATED, FINISHED, FAILED
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    finished_at = Column(DateTime)
    selected_information_sources = Column(String)  # e.g. enter_name_here, xxxx
    selected_labeling_tasks = Column(String)
    distinct_records = Column(Integer)  # records_hit
    result_count = Column(Integer)  # rlas

    record_label_associations = parent_to_child_relationship(
        Tablenames.WEAK_SUPERVISION_TASK,
        Tablenames.RECORD_LABEL_ASSOCIATION,
    )


# -------------------- KNOWLEDGE_BASE_ --------------------
class KnowledgeBase(Base):
    __tablename__ = Tablenames.KNOWLEDGE_BASE.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    description = Column(String)

    terms = parent_to_child_relationship(
        Tablenames.KNOWLEDGE_BASE,
        Tablenames.KNOWLEDGE_TERM,
    )


class KnowledgeTerm(Base):
    __tablename__ = Tablenames.KNOWLEDGE_TERM.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    knowledge_base_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.KNOWLEDGE_BASE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    value = Column(String)
    comment = Column(String)
    blacklisted = Column(Boolean, default=False)


class AdminMessage(Base):
    __tablename__ = Tablenames.ADMIN_MESSAGE.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    text = Column(String)
    level = Column(String, default=AdminMessageLevel.INFO.value)
    archived = Column(Boolean, default=False)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    archive_date = Column(DateTime)
    archived_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    archived_reason = Column(String)
    scheduled_date = Column(DateTime)


class TaskQueue(Base):
    # start without indexing since the idea is to remove on calculation start
    # only meant as persistent layer, queue itself accesses cache
    __tablename__ = Tablenames.TASK_QUEUE.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    task_type = Column(String)  # enum.TaskType e.g. EMBEDDING
    task_info = Column(JSON)
    # priority queue is for probable fast execution tasks (e.g. lf calculation)
    priority = Column(Boolean, default=False)
    is_active = Column(Boolean, default=False)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )


# --- COGNITION TABLES
class CognitionProject(Base):
    __tablename__ = Tablenames.PROJECT.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    name = Column(String)
    description = Column(String)
    color = Column(String)
    operator_routing_config = Column(JSON)
    state = Column(
        String, default=CognitionProjectState.CREATED.value
    )  # of type enums.CognitionProjectState.*.value
    facts_grouping_attribute = Column(String)
    interface_type = Column(String)

    customer_color_primary = Column(String, default="#18181b")
    customer_color_primary_only_accent = Column(Boolean, default=False)
    customer_color_secondary = Column(String, default="#9333ea")

    allow_file_upload = Column(Boolean, default=False)
    max_file_size_mb = Column(Float, default=3.0)
    useable_etl_configurations = Column(JSON)
    max_folder_size_mb = Column(Float, default=20.0)
    # holds e.g. show, admin macro setting etc.
    macro_config = Column(JSON)
    # options from <SVGIcon/> component - only visible with new UI selected (user setting)
    icon = Column(String, default="IconBolt")
    allow_conversation_sharing_organization = Column(Boolean, default=False)
    allow_conversation_sharing_global = Column(Boolean, default=False)


class CognitionStrategy(Base):
    __tablename__ = Tablenames.STRATEGY.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    name = Column(String)
    description = Column(String)
    complexity = Column(
        String, default=StrategyComplexity.SIMPLE.value
    )  # of type enums.StrategyComplexity.*.value
    order = Column(Integer, default=0)


class CognitionStrategyRequirement(Base):
    __tablename__ = Tablenames.STRATEGY_REQUIREMENT.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    strategy_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.STRATEGY.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    field = Column(String)
    description = Column(String)
    is_input = Column(Boolean, default=False)


class CognitionStrategyRequirementMappingOption(Base):
    __tablename__ = Tablenames.STRATEGY_REQUIREMENT_MAPPING_OPTION.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    strategy_requirement_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.STRATEGY_REQUIREMENT.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    value = Column(String)


class CognitionStrategyStep(Base):
    __tablename__ = Tablenames.STRATEGY_STEP.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    strategy_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.STRATEGY.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    name = Column(String)
    description = Column(String)
    step_type = Column(String)
    position = Column(Integer)
    config = Column(JSON)
    progress_text = Column(String)
    execute_if_source_code = Column(String)


class CognitionConversation(Base):
    __tablename__ = Tablenames.CONVERSATION.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    scope_dict = Column(JSON)
    header = Column(String)
    error = Column(String)
    has_tmp_files = Column(Boolean, default=False)
    archived = Column(Boolean, default=False)
    incognito_mode = Column(Boolean, default=False)


class CognitionMessage(Base):
    __tablename__ = Tablenames.MESSAGE.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    strategy_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.STRATEGY.value}.id", ondelete="SET NULL"),
        index=True,
    )
    conversation_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.CONVERSATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    question = Column(String)
    facts = Column(ARRAY(JSON))
    selection_widget = Column(ARRAY(JSON))
    answer = Column(String)

    feedback_value = Column(String)
    feedback_category = Column(String)
    feedback_message = Column(String)

    scope_dict_diff_previous_conversation = Column(JSON)
    scope_dict_diff_new = Column(JSON)
    version_id = Column(
        # pipeline version with which the message was created, can be null if the version isn't available anymore (e.g. deleted)
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.PIPELINE_VERSION.value}.id", ondelete="SET NULL"
        ),
        index=True,
    )
    # holds e.g. the project privacy report rating (high level) at message creation
    additional_data = Column(JSON)
    initiated_via = Column(String)  # of type enums.MessageInitiationType.*.value


class CognitionPipelineLogs(Base):
    __tablename__ = Tablenames.PIPELINE_LOGS.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    strategy_step_id = Column(
        UUID(as_uuid=True),
        # removed fkey constraint to ensure that a different pipeline version message can still be matched
        # ForeignKey(
        #     f"cognition.{Tablenames.STRATEGY_STEP.value}.id", ondelete="CASCADE"
        # ),
        index=True,
    )
    message_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.MESSAGE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    pipeline_step_type = Column(String)
    strategy_step_type = Column(String)

    has_error = Column(Boolean)
    scope_dict_diff_previous_message = Column(JSON)
    record_dict_diff_previous_message = Column(JSON)
    scope_dict_diff_new = Column(JSON)
    record_dict_diff_new = Column(JSON)
    content = Column(ARRAY(String))
    time_elapsed = Column(Float)
    skipped_step = Column(Boolean, default=False)
    iteration_number = Column(Integer)


class CognitionConsumptionLog(Base):
    __tablename__ = Tablenames.CONSUMPTION_LOG.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="SET NULL"),
        index=True,
    )
    strategy_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.STRATEGY.value}.id", ondelete="SET NULL"),
        index=True,
    )
    conversation_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.CONVERSATION.value}.id", ondelete="SET NULL"
        ),
        index=True,
    )
    message_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.MESSAGE.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    complexity = Column(String)  # of type enums.StrategyComplexity.*.value
    state = Column(String)  # of type enums.ConsumptionLogState.*.value
    project_name = Column(String)
    project_state = Column(String)  # of type enums.CognitionProjectState.*.value


class CognitionConsumptionSummary(Base):
    __tablename__ = Tablenames.CONSUMPTION_SUMMARY.value
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "project_id",
            "creation_date",
            "complexity",
            name="unique_summary",
        ),
        {"schema": "cognition"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="SET NULL"),
        index=True,
    )
    creation_date = Column(Date, default=sql.func.now(), index=True)
    project_name = Column(String)
    complexity = Column(String)  # of type enums.StrategyComplexity.*.value
    count = Column(Integer)


class CognitionEnvironmentVariable(Base):
    __tablename__ = Tablenames.ENVIRONMENT_VARIABLE.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    name = Column(String)
    description = Column(String)
    value = Column(String)
    is_secret = Column(Boolean)


class CognitionPersonalAccessToken(Base):
    __tablename__ = Tablenames.PERSONAL_ACCESS_TOKEN.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    name = Column(String)
    scope = Column(String)
    expires_at = Column(DateTime)
    last_used = Column(DateTime)
    token = Column(String)


class CognitionPersonalAccessTokenEtl(Base):
    __tablename__ = Tablenames.PERSONAL_ACCESS_TOKEN_ETL.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    name = Column(String)
    expires_at = Column(DateTime)
    last_used = Column(DateTime)
    token = Column(String)
    scopes = parent_to_child_relationship(
        Tablenames.PERSONAL_ACCESS_TOKEN_ETL,
        Tablenames.PERSONAL_ACCESS_TOKEN_SCOPE_ETL,
    )


class PersonalAccessTokenScopeEtl(Base):
    __tablename__ = Tablenames.PERSONAL_ACCESS_TOKEN_SCOPE_ETL.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = Column(DateTime, default=sql.func.now())
    scope = Column(String, default=TokenScope.READ_WRITE.value)
    subject = Column(String, default=TokenSubject.PROJECT.value)
    subject_id = Column(UUID(as_uuid=True))  # project_id or dataset_id
    token_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.PERSONAL_ACCESS_TOKEN_ETL.value}.id",
            ondelete="CASCADE",
        ),
        index=True,
    )


class PersonalAccessTokenActivityLogEtl(Base):
    __tablename__ = Tablenames.PERSONAL_ACCESS_TOKEN_ACTIVITY_LOG_ETL.value
    __table_args__ = (
        Index(
            f"idx_{Tablenames.PERSONAL_ACCESS_TOKEN_ACTIVITY_LOG_ETL.value}_created_at",
            "created_at",
            postgresql_using="brin",
        ),
        {"schema": "cognition"},
    )
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = Column(DateTime(timezone=True), default=sql.func.now())
    action = Column(String, index=True)
    quantity = Column(Integer, default=1)
    endpoint = Column(String)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"{Tablenames.ORGANIZATION.value}.id",
            ondelete="CASCADE",
        ),
        index=True,
    )
    token_scope_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.PERSONAL_ACCESS_TOKEN_SCOPE_ETL.value}.id",
            ondelete="SET NULL",
        ),
        index=True,
    )


class CognitionMarkdownDataset(Base):
    __tablename__ = Tablenames.MARKDOWN_DATASET.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    refinery_project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    name = Column(String)
    description = Column(String)

    # might want to index this in the future since it's based on an enum
    category_origin = Column(String)
    useable_etl_configurations = Column(JSON)


class CognitionMarkdownFile(Base):
    __tablename__ = Tablenames.MARKDOWN_FILE.value
    __table_args__ = (
        UniqueConstraint(
            "id",
            "etl_task_id",
            name=f"unique_{__tablename__}_etl_task_id",
        ),
        {"schema": "cognition"},
    )
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    dataset_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.MARKDOWN_DATASET.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    file_name = Column(String)
    content = Column(String)
    category_origin = Column(String)
    error = Column(String)
    state = Column(String)
    is_reviewed = Column(Boolean, default=False)
    meta_data = Column(JSON)

    etl_task_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"global.{Tablenames.ETL_TASK.value}.id", ondelete="SET NULL"),
        index=True,
    )


class FileTransformationLLMLogs(Base):
    __tablename__ = Tablenames.FILE_TRANSFORMATION_LLM_LOGS.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    file_transformation_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.FILE_TRANSFORMATION.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    finished_at = Column(DateTime)
    model_used = Column(String)
    input = Column(String)
    output = Column(String)
    error = Column(String)


class CognitionMacro(Base):
    __tablename__ = Tablenames.MACRO.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    macro_type = Column(String)  # enums.MacroType
    scope = Column(String, index=True)  # enums.MacroScope
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
        nullable=True,  # ADMIN MACROS dont have a org_id
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
        nullable=True,  # ADMIN or ORGANIZATION MACROS dont have a project_id
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    state = Column(String)  # enums.MacroState
    name = Column(String)
    description = Column(String)


class CognitionMacroNode(Base):
    __tablename__ = Tablenames.MACRO_NODE.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    macro_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.MACRO.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    is_root = Column(Boolean)  # to easily filter outside of config
    config = Column(JSON)
    # example config:
    # {
    #     "name": "dummy",
    #     "content_type": enums.MacroNodeContentType.CONVERSATION_QUESTION,
    #     "content": { "question": "hello"},
    #     "position": {"x": 0, "y": 0},
    # }


class CognitionMacroEdge(Base):
    __tablename__ = Tablenames.MACRO_EDGE.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    macro_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.MACRO.value}.id", ondelete="CASCADE"),
        index=True,
    )
    from_node_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.MACRO_NODE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    to_node_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.MACRO_NODE.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    config = Column(JSON)
    # example config:
    # {
    #  "condition_type": enums.MacroEdgeConditionType.LLM_SELECTION
    #  "condition":{ "option": "document is invoice" },
    # }


class CognitionMacroExecution(Base):
    __tablename__ = Tablenames.MACRO_EXECUTION.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    macro_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.MACRO.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    state = Column(String)  # MacroExecutionState

    # used for comparison groups. N files => 1 execution group
    # "who was started together"
    execution_group_id = Column(UUID(as_uuid=True), index=True, default=uuid.uuid4)
    # additional data for the execution, e.g. file name or project id if applicable
    meta_info = Column(JSON)


class CognitionMacroExecutionLink(Base):
    __tablename__ = Tablenames.MACRO_EXECUTION_LINK.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
        nullable=True,  # ADMIN MACROS dont have a org_id
    )
    execution_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.MACRO_EXECUTION.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    execution_node_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.MACRO_NODE.value}.id", ondelete="CASCADE"),
        index=True,
        nullable=True,  # e.g. for a conversation itself (hull, not the messages)
    )

    action = Column(String)  # CREATE, UPDATE, DELETE
    other_id_target = Column(String)  # enums.Tablenames, currently conversation/message
    other_id = Column(UUID(as_uuid=True), index=True)


class CognitionMacroExecutionSummary(Base):
    __tablename__ = Tablenames.MACRO_EXECUTION_SUMMARY.value
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "creation_month",
            "macro_type",
            name="unique_macro_summary",
        ),
        {"schema": "cognition"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    creation_month = Column(
        Date, default=sql.func.date_trunc("month", sql.func.now()), index=True
    )
    macro_type = Column(String)  # of type enums.MacroType
    execution_count = Column(Integer)
    processed_files_count = Column(Integer)


class FileReference(Base):
    __tablename__ = Tablenames.FILE_REFERENCE.value
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "hash",
            "file_size_bytes",
            name="unique_file_reference",
        ),
        {"schema": "cognition"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    hash = Column(
        String,
        index=True,
    )
    last_used = Column(DateTime, default=sql.func.now())
    minio_path = Column(String)
    bucket = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    file_size_bytes = Column(BigInteger)
    content_type = Column(String)
    original_file_name = Column(String)
    state = Column(String, default=FileCachingState.CREATED.value)
    meta_data = Column(JSON)


class FileExtraction(Base):
    __tablename__ = Tablenames.FILE_EXTRACTION.value
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "file_reference_id",
            "extraction_key",
            name="unique_file_extraction",
        ),
        {"schema": "cognition"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    file_reference_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.FILE_REFERENCE.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    extraction_key = Column(String)
    minio_path = Column(String)
    bucket = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    state = Column(String, default=FileCachingState.CREATED.value)


class CognitionPipelineVersion(Base):
    __tablename__ = Tablenames.PIPELINE_VERSION.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    version_type = Column(String, default=PipelineVersionType.AUTO_SAVE.value)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    pipeline_dump = Column(JSON)


class FileTransformation(Base):
    __tablename__ = Tablenames.FILE_TRANSFORMATION.value
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "file_extraction_id",
            "transformation_key",
            name="unique_file_transformation",
        ),
        {"schema": "cognition"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    file_extraction_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.FILE_EXTRACTION.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    transformation_key = Column(String)
    minio_path = Column(String)
    bucket = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    state = Column(String, default=FileCachingState.CREATED.value)


class GraphRAGIndex(Base):
    __tablename__ = Tablenames.GRAPHRAG_INDEX.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    description = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    state = Column(String)  # enum.GraphRAGIndexState
    error = Column(String)
    settings = Column(JSON)
    root_dir = Column(String)


class StepTemplates(Base):
    __tablename__ = Tablenames.STEP_TEMPLATES.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    description = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    config = Column(JSON)  # JSON schema for the step template
    # config contains all step configurations in an array & variable fields to be changed on useage
    # e.g.
    # {
    #     "variables": [
    # {"name": "Env var", "path": "[0].config.llmConfig.environmentVariable", "hasDefault": True, "defaultValue": "OpenAI Leo"},
    # {"name": "System Prompt", "path": "[0].config.templatePrompt", "hasDefault": False},
    # ],
    #     "steps": [{...},{...}]
    # }


class CognitionGroup(Base):
    __tablename__ = Tablenames.GROUP.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    description = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    meta_data = Column(JSON)


class CognitionGroupMember(Base):
    __tablename__ = Tablenames.GROUP_MEMBER.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    group_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.GROUP.value}.id", ondelete="CASCADE"),
        index=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())


class ETLConfigPresets(Base):
    __tablename__ = Tablenames.ETL_CONFIG_PRESET.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    description = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    etl_config = Column(JSON)  # full ETL config JSON schema for how to run the ETL
    add_config = Column(JSON)  # additional config for e.g. setting scope dict values


# =========================== Global tables ===========================
class GlobalWebsocketAccess(Base):
    # table to store prepared websocket configuration.
    # to ensure stateless communication, the configuration is stored in the database
    # an entry doesn't mean it will be used but can be used
    # example code runner that prepares the access but the custom code doesn't have to use it
    # entries should be cleared on startup

    __tablename__ = Tablenames.WEBSOCKET_ACCESS.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    config = Column(JSON)
    in_use = Column(Boolean, default=False)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )


class CustomerButton(Base):
    # table to configuration customer buttons

    __tablename__ = Tablenames.CUSTOMER_BUTTON.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
        # not part of p-key since a customer could have multiple
    )
    type = Column(String)  # enums.CustomerButtonType
    location = Column(String)  # enums.CustomerButtonLocation
    visible = Column(Boolean, default=False)  # for easy disable
    config = Column(JSON)  # changes based on type
    # e.g. for DATA_MAPPER
    # {
    #     "url":"http://localhost:9060/hdi/map-to-collect-data?key=abc123", # including access key for e.g. external mapper
    #     "icon":"<icon_name>",
    #     "tooltip":"Map results to HDI D&O Excel"
    # }

    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )


class EvaluationSet(Base):
    __tablename__ = Tablenames.EVALUATION_SET.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    question = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    record_ids = Column(JSON)


class EvaluationGroup(Base):
    __tablename__ = Tablenames.EVALUATION_GROUP.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    evaluation_set_ids = Column(JSON)


class EvaluationRun(Base):
    __tablename__ = Tablenames.EVALUATION_RUN.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    evaluation_group_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.EVALUATION_GROUP.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    embedding_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.EMBEDDING.value}.id", ondelete="SET NULL"),
        index=True,
    )
    state = Column(String)
    results = Column(JSON)
    meta_info = Column(JSON)


class PlaygroundQuestion(Base):
    __tablename__ = Tablenames.PLAYGROUND_QUESTION.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    question = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="CASCADE"),
        index=True,
    )
    """
    Playground question can be extended with the below properties to allow the following:
        - User can see questions with specific results relating to the embedding used
        - Can be used for comparison with new results using same question but different embedding
    """
    # embedding_id = Column(
    #     UUID(as_uuid=True),
    #     ForeignKey(f"{Tablenames.EMBEDDING.value}.id", ondelete="SET NULL"),
    #     index=True,
    # )
    # record_ids = Column(JSON)
    # meta_info = Column(JSON)


class FullAdminAccess(Base):
    __tablename__ = Tablenames.FULL_ADMIN_ACCESS.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String, unique=True)
    meta_info = Column(JSON)


class CognitionIntegration(Base):
    __tablename__ = Tablenames.INTEGRATION.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.PROJECT.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    updated_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    updated_at = Column(DateTime, onupdate=sql.func.now())
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    name = Column(String)
    description = Column(String)
    tokenizer = Column(String)
    state = Column(String)  # of type enums.CognitionMarkdownFileState.*.value
    type = Column(String)  # of type enums.CognitionIntegrationType.*.value
    config = Column(JSON)
    """JSON object that contains the configuration for the integration type.
    Examples:
        - For a webhook integration, it might contain the URL and headers.
        - For an API integration, it might contain the API key and endpoint.
        - For a database integration, it might contain the connection string and credentials.

    """

    llm_config = Column(JSON)
    error_message = Column(String)
    is_synced = Column(Boolean, nullable=True)
    last_synced_at = Column(DateTime)
    delta_criteria = Column(JSON)


class CognitionIntegrationAccess(Base):
    __tablename__ = Tablenames.INTEGRATION_ACCESS.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    integration_types = Column(
        ARRAY(String)
    )  # of type enums.CognitionIntegrationType.*.value


class IntegrationGithubFile(Base):
    __tablename__ = Tablenames.INTEGRATION_GITHUB_FILE.value
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "running_id",
            "source",
            "etl_task_id",
            name=f"unique_{__tablename__}_source",
        ),
        {"schema": "integration"},
    )
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    updated_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    updated_at = Column(DateTime, onupdate=sql.func.now())
    integration_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.INTEGRATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    running_id = Column(Integer, index=True)
    source = Column(String, index=True)
    minio_file_name = Column(String)
    error_message = Column(String)

    path = Column(String)
    sha = Column(String)
    code_language = Column(String)

    etl_task_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"global.{Tablenames.ETL_TASK.value}.id", ondelete="CASCADE"),
        index=True,
    )
    content = Column(String)


class IntegrationGithubIssue(Base):
    __tablename__ = Tablenames.INTEGRATION_GITHUB_ISSUE.value
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "running_id",
            "source",
            "etl_task_id",
            name=f"unique_{__tablename__}_source",
        ),
        {"schema": "integration"},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    updated_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    updated_at = Column(DateTime, onupdate=sql.func.now())
    integration_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.INTEGRATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    running_id = Column(Integer, index=True)
    source = Column(String, index=True)
    minio_file_name = Column(String)
    error_message = Column(String)

    url = Column(String)
    state = Column(String)
    assignee = Column(String)
    milestone = Column(String)
    number = Column(Integer)

    etl_task_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"global.{Tablenames.ETL_TASK.value}.id", ondelete="CASCADE"),
        index=True,
    )
    content = Column(String)


class IntegrationPdf(Base):
    __tablename__ = Tablenames.INTEGRATION_PDF.value
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "running_id",
            "source",
            "etl_task_id",
            name=f"unique_{__tablename__}_source",
        ),
        {"schema": "integration"},
    )
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    updated_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    updated_at = Column(DateTime, onupdate=sql.func.now())
    integration_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.INTEGRATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    running_id = Column(Integer, index=True)
    source = Column(String, index=True)
    minio_file_name = Column(String)
    error_message = Column(String)

    file_path = Column(String)
    page = Column(Integer)
    total_pages = Column(Integer)
    title = Column(String)

    etl_task_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"global.{Tablenames.ETL_TASK.value}.id", ondelete="CASCADE"),
        index=True,
    )
    content = Column(String)


class IntegrationSharepoint(Base):
    __tablename__ = Tablenames.INTEGRATION_SHAREPOINT.value
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "running_id",
            "source",
            "etl_task_id",
            name=f"unique_{__tablename__}_source",
        ),
        {"schema": "integration"},
    )
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    updated_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    updated_at = Column(DateTime, onupdate=sql.func.now())
    integration_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.INTEGRATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    running_id = Column(Integer, index=True)
    source = Column(String, index=True)
    minio_file_name = Column(String)
    error_message = Column(String)
    refinery_synced = Column(Boolean, default=False)

    extension = Column(String)
    object_id = Column(String)
    parent_path = Column(String)
    name = Column(String)
    web_url = Column(String)
    sharepoint_created_by = Column(String)
    modified_by = Column(String)
    created = Column(DateTime, default=None)
    modified = Column(DateTime, default=None)
    description = Column(String)
    size = Column(BigInteger)
    mime_type = Column(String)
    hashes = Column(JSON)
    permissions = Column(JSON)
    file_properties = Column(JSON)

    etl_task_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"global.{Tablenames.ETL_TASK.value}.id", ondelete="CASCADE"),
        index=True,
    )
    content = Column(String)


class IntegrationSharepointPropertySync(Base):
    __tablename__ = Tablenames.INTEGRATION_SHAREPOINT_PROPERTY_SYNC.value
    __table_args__ = (UniqueConstraint("integration_id"), {"schema": "integration"})
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    updated_at = Column(DateTime, onupdate=sql.func.now())
    integration_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.INTEGRATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    config = Column(JSON)  # JSON object containing the rules for property sync
    logs = Column(ARRAY(String))
    state = Column(String)


class IntegrationWebpage(Base):
    __tablename__ = Tablenames.INTEGRATION_WEBPAGE.value
    __table_args__ = (
        UniqueConstraint(
            "integration_id",
            "source",
            "etl_task_id",
            name=f"unique_{__tablename__}_source",
        ),
        {"schema": "integration"},
    )
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    updated_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    updated_at = Column(DateTime, onupdate=sql.func.now())
    integration_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.INTEGRATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    running_ids = Column(JSON)  # to allow multiple running ids for webpages
    source = Column(String, index=True)  # url
    minio_file_name = Column(String)
    error_message = Column(String)
    refinery_synced = Column(Boolean, default=False)

    title = Column(String)
    raw_markdown_content = Column(String)  # before any processing

    etl_task_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"global.{Tablenames.ETL_TASK.value}.id", ondelete="CASCADE"),
        index=True,
    )
    content = Column(String)


class CognitionConversationTag(Base):
    __tablename__ = Tablenames.CONVERSATION_TAG.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    name = Column(String)
    created_at = Column(DateTime, default=sql.func.now())
    config = Column(JSON)
    # JSON schema for the tag configuration, e.g. global tag, use for projects, maybe at some point color, sort_by (conv creation, last message creation, tag creation, conv header)


class CognitionConversationTagAssociation(Base):
    __tablename__ = Tablenames.CONVERSATION_TAG_ASSOCIATION.value
    __table_args__ = {"schema": "cognition"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.CONVERSATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    tag_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"cognition.{Tablenames.CONVERSATION_TAG.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())


class SumsTable(Base):
    __tablename__ = Tablenames.SUMS_TABLE.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sum_key = Column(String, index=True)  # e.g. enums.AdminQueries
    created_at = Column(DateTime, default=sql.func.now())
    data = Column(JSON)


class AdminQueryMessageSummary(Base):
    __tablename__ = Tablenames.ADMIN_QUERY_MESSAGE_SUMMARY.value
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "project_id",
            "day",
            name="unique_admin_query_msg_activity_summary",
        ),
        {"schema": "cognition"},
    )
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    day = Column(Date, nullable=False)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
    )
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.PROJECT.value}.id", ondelete="SET NULL"),
    )

    total_conversations = Column(Integer, default=0)
    total_messages = Column(Integer, default=0)
    messages_via_api = Column(Integer, default=0)
    messages_via_ui = Column(Integer, default=0)
    messages_via_macro = Column(Integer, default=0)
    confidential_messages = Column(Integer, default=0)
    kern_user_messages = Column(Integer, default=0)
    deleted_messages_by_user = Column(Integer, default=0)
    deleted_messages_by_system = Column(Integer, default=0)
    incognito_messages = Column(Integer, default=0)


class ReleaseNotification(Base):
    __tablename__ = Tablenames.RELEASE_NOTIFICATION.value
    __table_args__ = {"schema": "global"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    link = Column(String, nullable=False)
    config = Column(JSON)  # e.g. {"en": {"headline":"", "description":""}, "de": {...}}


class TimedExecutions(Base):
    __tablename__ = Tablenames.TIMED_EXECUTIONS.value
    __table_args__ = {"schema": "global"}
    time_key = Column(String, unique=True, primary_key=True)  # enums.TimedExecutionKey
    last_executed_at = Column(DateTime)


class EtlTask(Base):
    __tablename__ = Tablenames.ETL_TASK.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    original_file_name = Column(String)
    file_path = Column(String)
    file_size_bytes = Column(BigInteger)
    tokenizer = Column(String)

    # array of indivitual tasks to be executed including fallback etc.
    full_config = Column(JSON)  # full ETL config JSON schema for how to run the ETL

    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    state = Column(
        String, default=CognitionMarkdownFileState.QUEUE.value
    )  # of type enums.CognitionMarkdownFileState
    is_active = Column(Boolean, default=False)

    priority = Column(Integer, default=0)
    error_message = Column(String)
    meta_data = Column(JSON)

    full_config_hash = Column(String, index=True)
    is_stale = Column(Boolean, default=False)
    llm_ops = Column(JSON)
    updated_at = Column(DateTime, onupdate=sql.func.now())


class ConversationShare(Base):
    __tablename__ = Tablenames.CONVERSATION_SHARE.value
    __table_args__ = {"schema": "cognition"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.CONVERSATION.value}.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    shared_with = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    shared_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    can_copy = Column(Boolean, default=False)
    created_at = Column(DateTime, default=sql.func.now())


class ConversationGlobalShare(Base):
    __tablename__ = Tablenames.CONVERSATION_GLOBAL_SHARE.value
    __table_args__ = {"schema": "cognition"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"cognition.{Tablenames.CONVERSATION.value}.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    shared_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())


class InboxMailThread(Base):
    __tablename__ = Tablenames.INBOX_MAIL_THREAD.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    created_at = Column(DateTime, default=sql.func.now())
    subject = Column(String)
    meta_data = Column(JSON)
    is_important = Column(Boolean, default=False)
    progress_state = Column(
        String
    )  # of type enums. InboxMailThreadSupportProgressState *.value
    support_owner_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    is_admin_support_thread = Column(Boolean, default=False)


class InboxMail(Base):
    __tablename__ = Tablenames.INBOX_MAIL.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = Column(DateTime, default=sql.func.now())
    sender_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="SET NULL"),
        index=True,
    )
    thread_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"global.{Tablenames.INBOX_MAIL_THREAD.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    content = Column(String)


class InboxMailThreadAssociation(Base):
    __tablename__ = Tablenames.INBOX_MAIL_THREAD_ASSOCIATION.value
    __table_args__ = {"schema": "global"}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    thread_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"global.{Tablenames.INBOX_MAIL_THREAD.value}.id", ondelete="CASCADE"
        ),
        index=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    unread_mail_count = Column(Integer, default=0)
