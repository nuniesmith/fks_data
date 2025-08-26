"""
Data pipeline executor for running data processing workflows
"""

import importlib
import time
from typing import Any, Callable, Dict, List, Optional, Union

from framework.base.component import Component
from framework.common.exceptions import DataProcessingError, InvalidPipelineError
from loguru import logger

logger = logger.opt(colors=True)


class PipelineExecutor(Component):
    """
    Executor that runs a configured data processing pipeline.

    Takes the pipeline configuration from a PipelineBuilder and
    processes data through each step in sequence.
    """

    def __init__(self, steps: List[Dict[str, Any]]):
        """
        Initialize the pipeline executor.

        Args:
            steps: List of processing steps to execute
        """
        super().__init__()
        self.steps = steps
        self._validate_pipeline()
        logger.debug(f"Initialized pipeline executor with {len(steps)} steps")

    def _validate_pipeline(self) -> None:
        """
        Validate the pipeline configuration.

        Raises:
            InvalidPipelineError: If the pipeline configuration is invalid
        """
        if not self.steps:
            return  # Empty pipeline is valid but does nothing

        for i, step in enumerate(self.steps):
            if "type" not in step:
                raise InvalidPipelineError(f"Step {i} is missing 'type' field")

            step_type = step["type"]

            if step_type == "custom" and "processor" not in step:
                raise InvalidPipelineError(
                    f"Custom step {i} is missing 'processor' field"
                )

            if step_type == "resampler" and "target_interval" not in step:
                raise InvalidPipelineError(
                    f"Resampler step {i} is missing 'target_interval' field"
                )

            if step_type == "transformer" and "transformations" not in step:
                raise InvalidPipelineError(
                    f"Transformer step {i} is missing 'transformations' field"
                )

    def _create_processor(self, step: Dict[str, Any]) -> Any:
        """
        Create a processor instance for a pipeline step.

        Args:
            step: Step configuration

        Returns:
            Processor instance

        Raises:
            InvalidPipelineError: If the processor cannot be created
        """
        step_type = step["type"]

        if step_type == "cleaner":
            from domain.processing.layers.preprocessing.cleaner import DataCleaner

            return DataCleaner(**step.get("options", {}))

        elif step_type == "normalizer":
            from domain.processing.layers.preprocessing.normalizer import DataNormalizer

            return DataNormalizer(**step.get("options", {}))

        elif step_type == "resampler":
            from domain.processing.layers.preprocessing.resampler import DataResampler

            return DataResampler(step["target_interval"], **step.get("options", {}))

        elif step_type == "transformer":
            from domain.processing.layers.preprocessing.transformer import (
                DataTransformer,
            )

            return DataTransformer(step["transformations"], **step.get("options", {}))

        elif step_type == "custom":
            processor = step["processor"]

            if callable(processor):
                # If it's already a callable function
                return processor

            elif isinstance(processor, str):
                # If it's a string, try to import it
                try:
                    module_path, class_name = processor.rsplit(".", 1)
                    module = importlib.import_module(module_path)
                    processor_class = getattr(module, class_name)
                    return processor_class(**step.get("options", {}))
                except (ImportError, AttributeError, ValueError) as e:
                    raise InvalidPipelineError(
                        f"Failed to import custom processor '{processor}': {str(e)}"
                    )

            else:
                raise InvalidPipelineError(
                    f"Invalid custom processor type: {type(processor)}"
                )

        else:
            raise InvalidPipelineError(f"Unknown step type: {step_type}")

    def execute(self, data: Any) -> Any:
        """
        Execute the pipeline on the input data.

        Args:
            data: Input data to process

        Returns:
            Processed data

        Raises:
            DataProcessingError: If there's an error during processing
        """
        if not self.steps:
            logger.debug("Executing empty pipeline (no-op)")
            return data

        result = data
        start_time = time.time()

        try:
            for i, step in enumerate(self.steps):
                step_start = time.time()
                step_type = step["type"]

                logger.debug(
                    f"Executing pipeline step {i+1}/{len(self.steps)} ({step_type})"
                )

                # Create and execute the processor
                processor = self._create_processor(step)
                result = (
                    processor.process(result)
                    if hasattr(processor, "process")
                    else processor(result)
                )

                step_duration = time.time() - step_start
                logger.debug(
                    f"Step {i+1} ({step_type}) completed in {step_duration:.3f}s"
                )

            total_duration = time.time() - start_time
            logger.info(f"Pipeline execution completed in {total_duration:.3f}s")

            return result

        except Exception as e:
            logger.error(f"Error executing pipeline: {str(e)}")
            raise DataProcessingError(f"Pipeline execution failed: {str(e)}")

    def __call__(self, data: Any) -> Any:
        """
        Call operator for executing the pipeline.

        Allows the executor to be used as a callable function.

        Args:
            data: Input data to process

        Returns:
            Processed data
        """
        return self.execute(data)
