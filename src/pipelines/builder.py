"""
Data pipeline builder for constructing data processing workflows
"""

from typing import Any, Callable, Dict, List, Optional, Union
from loguru import logger

from framework.base.component import Component
from services.data.pipelines.executor import PipelineExecutor

logger = logger.opt(colors=True)


class PipelineBuilder(Component):
    """
    Builder for creating data processing pipelines.

    Provides a fluent interface for constructing pipelines by chaining
    together different data processing steps.
    """

    def __init__(self):
        """Initialize a new empty pipeline builder."""
        super().__init__()
        self.steps: List[Dict[str, Any]] = []
        logger.debug("Initialized new pipeline builder")

    def clean(self, **options) -> "PipelineBuilder":
        """
        Add a data cleaning step to the pipeline.

        Args:
            **options: Options to pass to the DataCleaner

        Returns:
            Self for method chaining
        """
        self.steps.append({"type": "cleaner", "options": options})
        logger.debug(f"Added cleaning step to pipeline with options: {options}")
        return self

    def normalize(self, **options) -> "PipelineBuilder":
        """
        Add a data normalization step to the pipeline.

        Args:
            **options: Options to pass to the DataNormalizer

        Returns:
            Self for method chaining
        """
        self.steps.append({"type": "normalizer", "options": options})
        logger.debug(f"Added normalization step to pipeline with options: {options}")
        return self

    def resample(self, target_interval: str, **options) -> "PipelineBuilder":
        """
        Add a data resampling step to the pipeline.

        Args:
            target_interval: Target time interval to resample data to
            **options: Additional options to pass to the DataResampler

        Returns:
            Self for method chaining
        """
        self.steps.append(
            {
                "type": "resampler",
                "target_interval": target_interval,
                "options": options,
            }
        )
        logger.debug(
            f"Added resampling step to pipeline (target_interval={target_interval})"
        )
        return self

    def transform(
        self, transformations: List[Dict[str, Any]], **options
    ) -> "PipelineBuilder":
        """
        Add a data transformation step to the pipeline.

        Args:
            transformations: List of transformation specifications
            **options: Additional options to pass to the DataTransformer

        Returns:
            Self for method chaining
        """
        self.steps.append(
            {
                "type": "transformer",
                "transformations": transformations,
                "options": options,
            }
        )
        logger.debug(
            f"Added transformation step to pipeline with {len(transformations)} transformations"
        )
        return self

    def custom(self, processor: Union[Callable, str], **options) -> "PipelineBuilder":
        """
        Add a custom processing step to the pipeline.

        Args:
            processor: Custom processor function or class name
            **options: Options to pass to the processor

        Returns:
            Self for method chaining
        """
        self.steps.append(
            {"type": "custom", "processor": processor, "options": options}
        )
        processor_name = processor if isinstance(processor, str) else processor.__name__
        logger.debug(f"Added custom processor '{processor_name}' to pipeline")
        return self

    def build(self) -> PipelineExecutor:
        """
        Build and return a pipeline executor.

        Returns:
            A configured PipelineExecutor ready to process data
        """
        if not self.steps:
            logger.warning("Building empty pipeline with no processing steps")

        executor = PipelineExecutor(self.steps)
        logger.debug(f"Built pipeline with {len(self.steps)} steps")
        return executor

    def reset(self) -> "PipelineBuilder":
        """
        Reset the builder to an empty state.

        Returns:
            Self for method chaining
        """
        self.steps = []
        logger.debug("Reset pipeline builder to empty state")
        return self

    def clone(self) -> "PipelineBuilder":
        """
        Create a copy of this builder with the same steps.

        Returns:
            A new PipelineBuilder with the same configuration
        """
        clone = PipelineBuilder()
        # Perform a deep copy of the steps
        import copy

        clone.steps = copy.deepcopy(self.steps)
        logger.debug(f"Cloned pipeline with {len(self.steps)} steps")
        return clone
