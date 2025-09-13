"""
This file contains the transformer for the Supabase metadata extraction application.
The transformer is responsible for transforming the raw metadata into the Atlan Type.

Read More: ./models/README.md
"""

from typing import Any, Dict, Optional, Type

from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.transformers.atlas import AtlasTransformer
from application_sdk.transformers.common.utils import build_atlas_qualified_name

logger = get_logger(__name__)


class SupabaseDatabase:
    """Represents a Supabase database entity in Atlan.

    This class handles the transformation of Supabase database metadata into Atlan entity format.
    """

    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Supabase database metadata into Atlan entity attributes.

        Args:
            obj: Dictionary containing the raw Supabase database metadata.

        Returns:
            Dict[str, Any]: Dictionary containing the transformed attributes and custom attributes.
        """
        attributes = {
            "name": obj.get("database_name", ""),
            "qualifiedName": build_atlas_qualified_name(
                obj.get("connection_qualified_name", ""), obj.get("database_name", "")
            ),
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
        }
        return {
            "attributes": attributes,
            "custom_attributes": {
                "databaseType": "supabase",
                "isCloudHosted": True,
            },
        }


class SupabaseSchema:
    """Represents a Supabase schema entity in Atlan.

    This class handles the transformation of Supabase schema metadata into Atlan entity format.
    """

    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Supabase schema metadata into Atlan entity attributes.

        Args:
            obj: Dictionary containing the raw Supabase schema metadata.

        Returns:
            Dict[str, Any]: Dictionary containing the transformed attributes and custom attributes.
        """
        attributes = {
            "name": obj.get("schema_name", ""),
            "qualifiedName": build_atlas_qualified_name(
                obj.get("connection_qualified_name", ""),
                obj.get("database_name", ""),
                obj.get("schema_name", ""),
            ),
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
            "databaseName": obj.get("database_name", ""),
            "tableCount": obj.get("table_count", 0),
            "viewCount": obj.get("view_count", 0),
        }
        return {
            "attributes": attributes,
            "custom_attributes": {
                "schemaOwner": obj.get("schema_owner", ""),
                "schemaDescription": obj.get("schema_description", ""),
                "schemaType": obj.get("schema_type", "other"),
            },
        }


class SupabaseTable:
    """Represents a Supabase table entity in Atlan.

    This class handles the transformation of Supabase table metadata into Atlan entity format.
    """

    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Supabase table metadata into Atlan entity attributes.

        Args:
            obj: Dictionary containing the raw Supabase table metadata.

        Returns:
            Dict[str, Any]: Dictionary containing the transformed attributes and custom attributes.
        """
        attributes = {
            "name": obj.get("table_name", ""),
            "schemaName": obj.get("table_schema", ""),
            "databaseName": obj.get("table_catalog", ""),
            "qualifiedName": build_atlas_qualified_name(
                obj.get("connection_qualified_name", ""),
                obj.get("table_catalog", ""),
                obj.get("table_schema", ""),
                obj.get("table_name", ""),
            ),
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
        }
        return {
            "attributes": attributes,
            "custom_attributes": {
                "tableType": obj.get("table_type", ""),
                "tableSize": obj.get("table_size", ""),
                "rowCount": obj.get("row_count", 0),
                "deadRowCount": obj.get("dead_row_count", 0),
                "lastAnalyze": obj.get("last_analyze", ""),
                "lastAutoanalyze": obj.get("last_autoanalyze", ""),
                "lastVacuum": obj.get("last_vacuum", ""),
                "lastAutovacuum": obj.get("last_autovacuum", ""),
                "hasRowLevelSecurity": obj.get("has_row_level_security", False),
                "tableCategory": obj.get("table_category", "other"),
            },
        }


class SupabaseColumn:
    """Represents a Supabase column entity in Atlan.

    This class handles the transformation of Supabase column metadata into Atlan entity format.
    """

    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Supabase column metadata into Atlan entity attributes.

        Args:
            obj: Dictionary containing the raw Supabase column metadata.

        Returns:
            Dict[str, Any]: Dictionary containing the transformed attributes and custom attributes.
        """
        attributes = {
            "name": obj.get("column_name", ""),
            "qualifiedName": build_atlas_qualified_name(
                obj.get("connection_qualified_name", ""),
                obj.get("table_catalog", ""),
                obj.get("table_schema", ""),
                obj.get("table_name", ""),
                obj.get("column_name", ""),
            ),
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
            "tableName": obj.get("table_name", ""),
            "schemaName": obj.get("table_schema", ""),
            "databaseName": obj.get("table_catalog", ""),
            "isNullable": obj.get("is_nullable", "NO") == "YES",
            "dataType": obj.get("data_type", ""),
            "order": obj.get("ordinal_position", 1),
        }

        custom_attributes = {
            "columnDefault": obj.get("column_default", ""),
            "characterMaximumLength": obj.get("character_maximum_length", ""),
            "characterOctetLength": obj.get("character_octet_length", ""),
            "numericPrecision": obj.get("numeric_precision", ""),
            "numericPrecisionRadix": obj.get("numeric_precision_radix", ""),
            "numericScale": obj.get("numeric_scale", ""),
            "datetimePrecision": obj.get("datetime_precision", ""),
            "intervalType": obj.get("interval_type", ""),
            "intervalPrecision": obj.get("interval_precision", ""),
            "characterSetCatalog": obj.get("character_set_catalog", ""),
            "characterSetSchema": obj.get("character_set_schema", ""),
            "characterSetName": obj.get("character_set_name", ""),
            "collationCatalog": obj.get("collation_catalog", ""),
            "collationSchema": obj.get("collation_schema", ""),
            "collationName": obj.get("collation_name", ""),
            "domainCatalog": obj.get("domain_catalog", ""),
            "domainSchema": obj.get("domain_schema", ""),
            "domainName": obj.get("domain_name", ""),
            "udtCatalog": obj.get("udt_catalog", ""),
            "udtSchema": obj.get("udt_schema", ""),
            "udtName": obj.get("udt_name", ""),
            "scopeCatalog": obj.get("scope_catalog", ""),
            "scopeSchema": obj.get("scope_schema", ""),
            "scopeName": obj.get("scope_name", ""),
            "maximumCardinality": obj.get("maximum_cardinality", ""),
            "dtdIdentifier": obj.get("dtd_identifier", ""),
            "isSelfReferencing": obj.get("is_self_referencing", ""),
            "isIdentity": obj.get("is_identity", ""),
            "identityGeneration": obj.get("identity_generation", ""),
            "identityStart": obj.get("identity_start", ""),
            "identityIncrement": obj.get("identity_increment", ""),
            "identityMaximum": obj.get("identity_maximum", ""),
            "identityMinimum": obj.get("identity_minimum", ""),
            "identityCycle": obj.get("identity_cycle", ""),
            "isGenerated": obj.get("is_generated", ""),
            "generationExpression": obj.get("generation_expression", ""),
            "isUpdatable": obj.get("is_updatable", ""),
            "columnDescription": obj.get("column_description", ""),
            "columnCategory": obj.get("column_category", "standard_column"),
        }

        return {
            "attributes": attributes,
            "custom_attributes": custom_attributes,
        }


class SupabaseAtlasTransformer(AtlasTransformer):
    def __init__(self, connector_name: str, tenant_id: str, **kwargs: Any):
        super().__init__(connector_name, tenant_id, **kwargs)

        self.entity_class_definitions["DATABASE"] = SupabaseDatabase
        self.entity_class_definitions["SCHEMA"] = SupabaseSchema
        self.entity_class_definitions["TABLE"] = SupabaseTable
        self.entity_class_definitions["COLUMN"] = SupabaseColumn

    def transform_row(
        self,
        typename: str,
        data: Dict[str, Any],
        workflow_id: str,
        workflow_run_id: str,
        entity_class_definitions: Dict[str, Type[Any]] | None = None,
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]:
        """Transform metadata into an Atlas entity.

        This method transforms the provided metadata into an Atlas entity based on
        the specified type. It also enriches the entity with workflow metadata.

        Args:
            typename (str): Type of the entity to create.
            data (Dict[str, Any]): Metadata to transform.
            workflow_id (str): ID of the workflow.
            workflow_run_id (str): ID of the workflow run.
            entity_class_definitions (Dict[str, Type[Any]], optional): Custom entity
                class definitions. Defaults to None.
            **kwargs: Additional keyword arguments.

        Returns:
            Optional[Dict[str, Any]]: The transformed entity as a dictionary, or None
                if transformation fails.

        Raises:
            Exception: If there's an error during entity deserialization.
        """
        typename = typename.upper()
        self.entity_class_definitions = (
            entity_class_definitions or self.entity_class_definitions
        )

        connection_qualified_name = kwargs.get("connection_qualified_name", None)
        connection_name = kwargs.get("connection_name", None)

        data.update(
            {
                "connection_qualified_name": connection_qualified_name,
                "connection_name": connection_name,
            }
        )

        creator = self.entity_class_definitions.get(typename)
        if creator:
            try:
                entity_attributes = creator.get_attributes(data)
                # enrich the entity with workflow metadata
                enriched_data = self._enrich_entity_with_metadata(
                    workflow_id, workflow_run_id, data
                )

                entity_attributes["attributes"].update(enriched_data["attributes"])
                entity_attributes["custom_attributes"].update(
                    enriched_data["custom_attributes"]
                )

                entity = {
                    "typeName": typename,
                    "attributes": entity_attributes["attributes"],
                    "customAttributes": entity_attributes["custom_attributes"],
                    "status": "ACTIVE",
                }

                return entity
            except Exception as e:
                logger.error(
                    "Error transforming {} entity: {}",
                    typename,
                    str(e),
                    extra={"data": data},
                )
                return None
        else:
            logger.error(f"Unknown typename: {typename}")
            return None
