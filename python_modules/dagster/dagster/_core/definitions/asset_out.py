from typing import Any, Mapping, NamedTuple, Optional, Sequence, Type, Union

import dagster._check as check
from dagster._annotations import PublicAttr, experimental_param
from dagster._core.definitions.asset_dep import AssetDep
from dagster._core.definitions.asset_spec import AssetSpec
from dagster._core.definitions.auto_materialize_policy import AutoMaterializePolicy
from dagster._core.definitions.backfill_policy import BackfillPolicy
from dagster._core.definitions.declarative_automation.automation_condition import (
    AutomationCondition,
)
from dagster._core.definitions.events import (
    AssetKey,
    CoercibleToAssetKey,
    CoercibleToAssetKeyPrefix,
)
from dagster._core.definitions.freshness_policy import FreshnessPolicy
from dagster._core.definitions.input import NoValueSentinel
from dagster._core.definitions.output import Out
from dagster._core.definitions.utils import (
    DEFAULT_IO_MANAGER_KEY,
    resolve_automation_condition,
    validate_tags_strict,
)
from dagster._core.types.dagster_type import DagsterType, resolve_dagster_type
from dagster._utils.warnings import disable_dagster_warnings


@experimental_param(param="owners")
@experimental_param(param="tags")
class AssetOut(
    NamedTuple(
        "_AssetOut",
        [
            ("key", PublicAttr[Optional[AssetKey]]),
            ("key_prefix", PublicAttr[Optional[Sequence[str]]]),
            ("metadata", PublicAttr[Optional[Mapping[str, Any]]]),
            ("io_manager_key", PublicAttr[str]),
            ("description", PublicAttr[Optional[str]]),
            ("is_required", PublicAttr[bool]),
            ("dagster_type", PublicAttr[Union[DagsterType, Type[NoValueSentinel]]]),
            ("group_name", PublicAttr[Optional[str]]),
            ("code_version", PublicAttr[Optional[str]]),
            ("freshness_policy", PublicAttr[Optional[FreshnessPolicy]]),
            ("automation_condition", PublicAttr[Optional[AutomationCondition]]),
            ("backfill_policy", PublicAttr[Optional[BackfillPolicy]]),
            ("owners", PublicAttr[Optional[Sequence[str]]]),
            ("tags", PublicAttr[Optional[Mapping[str, str]]]),
        ],
    )
):
    """Defines one of the assets produced by a :py:func:`@multi_asset <multi_asset>`.

    Attributes:
        key_prefix (Optional[Union[str, Sequence[str]]]): If provided, the asset's key is the
            concatenation of the key_prefix and the asset's name. When using ``@multi_asset``, the
            asset name defaults to the key of the "outs" dictionary Only one of the "key_prefix" and
            "key" arguments should be provided.
        key (Optional[Union[str, Sequence[str], AssetKey]]): The asset's key. Only one of the
            "key_prefix" and "key" arguments should be provided.
        dagster_type (Optional[Union[Type, DagsterType]]]):
            The type of this output. Should only be set if the correct type can not
            be inferred directly from the type signature of the decorated function.
        description (Optional[str]): Human-readable description of the output.
        is_required (bool): Whether the presence of this field is required. (default: True)
        io_manager_key (Optional[str]): The resource key of the IO manager used for this output.
            (default: "io_manager").
        metadata (Optional[Dict[str, Any]]): A dict of the metadata for the output.
            For example, users can provide a file path if the data object will be stored in a
            filesystem, or provide information of a database table when it is going to load the data
            into the table.
        group_name (Optional[str]): A string name used to organize multiple assets into groups. If
            not provided, the name "default" is used.
        code_version (Optional[str]): The version of the code that generates this asset.
        freshness_policy (Optional[FreshnessPolicy]): (Deprecated) A policy which indicates how up
            to date this asset is intended to be.
        automation_condition (Optional[AutomationCondition]): AutomationCondition to apply to the
            specified asset.
        backfill_policy (Optional[BackfillPolicy]): BackfillPolicy to apply to the specified asset.
        owners (Optional[Sequence[str]]): A list of strings representing owners of the asset. Each
            string can be a user's email address, or a team name prefixed with `team:`,
            e.g. `team:finops`.
        tags (Optional[Mapping[str, str]]): Tags for filtering and organizing. These tags are not
            attached to runs of the asset.
    """

    def __new__(
        cls,
        key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
        key: Optional[CoercibleToAssetKey] = None,
        dagster_type: Union[Type, DagsterType] = NoValueSentinel,
        description: Optional[str] = None,
        is_required: bool = True,
        io_manager_key: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        group_name: Optional[str] = None,
        code_version: Optional[str] = None,
        freshness_policy: Optional[FreshnessPolicy] = None,
        automation_condition: Optional[AutomationCondition] = None,
        backfill_policy: Optional[BackfillPolicy] = None,
        owners: Optional[Sequence[str]] = None,
        tags: Optional[Mapping[str, str]] = None,
        # TODO: FOU-243
        auto_materialize_policy: Optional[AutoMaterializePolicy] = None,
    ):
        if isinstance(key_prefix, str):
            key_prefix = [key_prefix]

        return super(AssetOut, cls).__new__(
            cls,
            key=AssetKey.from_coercible(key) if key is not None else None,
            key_prefix=check.opt_list_param(key_prefix, "key_prefix", of_type=str),
            dagster_type=(
                NoValueSentinel
                if dagster_type is NoValueSentinel
                else resolve_dagster_type(dagster_type)
            ),
            description=check.opt_str_param(description, "description"),
            is_required=check.bool_param(is_required, "is_required"),
            io_manager_key=check.opt_str_param(
                io_manager_key, "io_manager_key", default=DEFAULT_IO_MANAGER_KEY
            ),
            metadata=check.opt_mapping_param(metadata, "metadata", key_type=str),
            group_name=check.opt_str_param(group_name, "group_name"),
            code_version=check.opt_str_param(code_version, "code_version"),
            freshness_policy=check.opt_inst_param(
                freshness_policy, "freshness_policy", FreshnessPolicy
            ),
            automation_condition=check.opt_inst_param(
                resolve_automation_condition(automation_condition, auto_materialize_policy),
                "automation_condition",
                AutomationCondition,
            ),
            backfill_policy=check.opt_inst_param(
                backfill_policy, "backfill_policy", BackfillPolicy
            ),
            owners=check.opt_sequence_param(owners, "owners", of_type=str),
            tags=validate_tags_strict(tags),
        )

    def to_out(self) -> Out:
        return Out(
            dagster_type=self.dagster_type,
            description=self.description,
            metadata=self.metadata,
            is_required=self.is_required,
            io_manager_key=self.io_manager_key,
            code_version=self.code_version,
        )

    def to_spec(self, key: AssetKey, deps: Sequence[AssetDep]) -> AssetSpec:
        with disable_dagster_warnings():
            return AssetSpec.dagster_internal_init(
                key=key,
                metadata=self.metadata,
                description=self.description,
                skippable=not self.is_required,
                group_name=self.group_name,
                code_version=self.code_version,
                freshness_policy=self.freshness_policy,
                automation_condition=self.automation_condition,
                owners=self.owners,
                tags=self.tags,
                deps=deps,
                auto_materialize_policy=None,
                partitions_def=None,
                kinds=None,
            )

    @property
    def auto_materialize_policy(self) -> Optional[AutoMaterializePolicy]:
        return (
            self.automation_condition.as_auto_materialize_policy()
            if self.automation_condition
            else None
        )
