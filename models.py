import uuid

from .enums import (
    CascadeBehaviour,
    NotificationState,
    Tablenames,
    Notification,
    UploadStates,
    PayloadState,
    SliceTypes,
    UserRoles,
)
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    sql,
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


# -------------------- GLOBAL_ --------------------
class AppVersion(Base):
    __tablename__ = Tablenames.APP_VERSION.value
    service = Column(String, primary_key=True)
    installed_version = Column(String)  # local/installed Tag
    remote_version = Column(String)  # latest GitHub Tag
    last_checked = Column(DateTime)


class Comment(Base):
    __tablename__ = Tablenames.COMMENT.value
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
        ForeignKey(f"{Tablenames.USER.value}.id"),
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


class User(Base):
    __tablename__ = Tablenames.USER.value
    id = Column(UUID(as_uuid=True), primary_key=True)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.ORGANIZATION.value}.id", ondelete="CASCADE"),
        index=True,
    )
    role = Column(String, default=UserRoles.ENGINEER.value)  # enum UserRoles
    notifications = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.NOTIFICATION,
    )
    information_sources = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.INFORMATION_SOURCE,
    )
    user_queries = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.USER_SESSIONS,
    )
    user_record_label_associations = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.RECORD_LABEL_ASSOCIATION,
    )
    user_activities = parent_to_child_relationship(
        Tablenames.USER,
        Tablenames.USER_ACTIVITY,
        order_by="created_at.desc()",
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
        Tablenames.COMMENT,
        order_by="created_at.desc()",
    )


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
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    is_locked = Column(Boolean, default=False)


class UserActivity(Base):
    __tablename__ = Tablenames.USER_ACTIVITY.value
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_by = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    activity = Column(JSON)
    created_at = Column(DateTime)
    from_backup = Column(Boolean)


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
    level = Column(String, default=Notification.INFO.value)
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
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
        index=True,
    )
    state = Column(String, default=UploadStates.CREATED.value)
    progress = Column(Float, default=0.0)
    started_at = Column(DateTime, default=sql.func.now())
    finished_at = Column(DateTime)
    file_name = Column(String)
    file_type = Column(String)
    file_import_options = Column(String)


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
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
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
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
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
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
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
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
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
    name = Column(String)
    custom = Column(
        Boolean
    )  # custom = provided by user at transfer, not custom = calculated via embedding-service
    type = Column(String)  # find allowed expressions in enums.EmbeddingType
    state = Column(String)  # set by embedding service
    similarity_threshold = Column(Float)  # set by neural search

    tensors = parent_to_child_relationship(
        Tablenames.EMBEDDING,
        Tablenames.EMBEDDING_TENSOR,
    )


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
        ForeignKey(f"{Tablenames.USER.value}.id", ondelete="CASCADE"),
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
