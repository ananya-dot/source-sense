"""
Main entry point for the Supabase metadata extraction application.

This module initializes and runs the Supabase metadata extraction application,
setting up the workflow, worker, and server components.
"""

import asyncio

from app.activities import SupabaseMetadataExtractionActivities
from app.clients import SupabaseClient
from app.transformer import SupabaseAtlasTransformer
from app.workflows import SupabaseMetadataExtractionWorkflow
from application_sdk.application.metadata_extraction.sql import (
    BaseSQLMetadataExtractionApplication,
)
from application_sdk.common.error_codes import ApiError
from application_sdk.constants import APPLICATION_NAME
from application_sdk.observability.decorators.observability_decorator import (
    observability,
)
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.observability.metrics_adaptor import get_metrics
from application_sdk.observability.traces_adaptor import get_traces

logger = get_logger(__name__)
metrics = get_metrics()
traces = get_traces()


@observability(logger=logger, metrics=metrics, traces=traces)
async def main():
    try:
        # Initialize the application
        application = BaseSQLMetadataExtractionApplication(
            name=APPLICATION_NAME,
            client_class=SupabaseClient,
            transformer_class=SupabaseAtlasTransformer,
        )

        await application.setup_workflow(
            workflow_and_activities_classes=[
                (SupabaseMetadataExtractionWorkflow, SupabaseMetadataExtractionActivities)
            ],
        )

        # Start the worker
        await application.start_worker()

        # Setup the application server
        await application.setup_server(
            workflow_class=SupabaseMetadataExtractionWorkflow,
        )

        # Add static file serving
        from fastapi.staticfiles import StaticFiles
        from fastapi import FastAPI
        
        # Get the FastAPI app instance and mount static files
        if hasattr(application, 'app') and isinstance(application.app, FastAPI):
            application.app.mount("/static", StaticFiles(directory="frontend/static"), name="static")

        # Start the application server
        await application.start_server()

    except ApiError:
        logger.error(f"{ApiError.SERVER_START_ERROR}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
