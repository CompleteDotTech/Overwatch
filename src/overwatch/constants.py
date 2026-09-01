"""Shared Overwatch constants and parsing expressions."""

import re
from zoneinfo import ZoneInfo

DEFAULT_ZYMTRACE_PROJECT_ID = "00000000-0000-0000-0000-000000000000"
CLOUDWATCH_LOG_GROUP = "/skypilot/research-training"
CLOUDWATCH_REGION = "us-west-2"
TOKENS_PER_SECOND_EMA_ALPHA = 0.2
RESOURCE_HISTORY_DAYS = 2
BILLING_HISTORY_DAYS = 30
PACIFIC_TIME = ZoneInfo("America/Los_Angeles")
ACTIVE_SKY_STATUSES = {
    "PENDING",
    "SUBMITTED",
    "STARTING",
    "RUNNING",
    "WINDING_DOWN",
    "RECOVERING",
    "CANCELLING",
}
ACTIVE_RESOURCE_STATUSES = ACTIVE_SKY_STATUSES | {"AUTOSTOPPING", "INIT", "UP"}
TERMINAL_SKY_STATUSES = {
    "SUCCEEDED",
    "CANCELLED",
    "FAILED",
    "FAILED_SETUP",
    "FAILED_PRECHECKS",
    "FAILED_NO_RESOURCE",
    "FAILED_CONTROLLER",
}
HAIKU_SUFFIX_RE = re.compile(r"__[a-z]+_[a-z]+$")
GPU_RESOURCE_RE = re.compile(r"\[([^]:]+):(\d+(?:\.\d+)?)\]")
NODE_COUNT_RE = re.compile(r"^(\d+)x\[")
CLOUD_CONFIG_RE = re.compile(r"(?:gcs?|r2)://\S+/train_config\.yaml$")
AWS_CLUSTER_LINK_RE = re.compile(r"tag:ray-cluster-name=([^&]+)")
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
ANSI_SGR_RE = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_COLORS = (
    "#000000",
    "#cd3131",
    "#0dbc79",
    "#e5e510",
    "#2472c8",
    "#bc3fbc",
    "#11a8cd",
    "#e5e5e5",
    "#666666",
    "#f14c4c",
    "#23d18b",
    "#f5f543",
    "#3b8eea",
    "#d670d6",
    "#29b8db",
    "#ffffff",
)
TRAIN_PROGRESS_RE = re.compile(
    r"train .*?\[e(?P<epoch>\d+)/(?P<n_epochs>\d+) b(?P<batch>\d+)/(?P<batches_per_epoch>\d+)"
    r" .*?\[s(?P<completed>\d+)/(?P<total>\d+) .*?\].*?"
    r"\[(?P<elapsed>[^<\]]+)<(?P<remaining>.+?)\s+[\d.eE+-]+ex/s "
    r"(?P<tokens_per_second>[\d,]+)tok/s"
)
