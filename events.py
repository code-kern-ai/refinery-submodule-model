import re
from dataclasses import dataclass


class Event:
    @classmethod
    def event_name(cls):
        # transforms the class name so that it is better readable in MixPanel,
        # e.g. CreateProject becomes "Create Project"
        matches = re.finditer(
            ".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", cls.__name__
        )
        return " ".join([m.group(0) for m in matches])


class SignUp(Event):
    pass


@dataclass
class AddNotification(Event):
    Level: str
    Message: str


@dataclass
class CreateProject(Event):
    Name: str
    Description: str


@dataclass
class UploadRecords(Event):
    ProjectName: str
    Records: int


@dataclass
class AddLabelingTask(Event):
    ProjectName: str
    Name: str
    Type: str


@dataclass
class AddLabel(Event):
    ProjectName: str
    LabelingTaskName: str
    Name: str


@dataclass
class AddLabelsToRecord(Event):
    ProjectName: str
    Type: str


@dataclass
class AddInformationSourceRun(Event):
    ProjectName: str
    Type: str
    Code: str
    Logs: str
    RunTime: float


@dataclass
class AddWeakSupervisionRun(Event):
    ProjectName: str
    RunTime: float


@dataclass
class AppNavigation(Event):
    old: str
    new: str
    name: str
