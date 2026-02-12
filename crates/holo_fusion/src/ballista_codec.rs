//! Ballista logical extension codec wrapper for HoloFusion table providers.
//!
//! Ballista already knows how to encode most DataFusion plan extensions. This
//! module adds custom encoding/decoding for `HoloStoreTableProvider` so plans
//! can be distributed while preserving storage-specific provider state.

use std::sync::Arc;

use anyhow::Context;
use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result as DFResult, SchemaExt};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::logical_expr::{AggregateUDF, Extension, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::metrics::PushdownMetrics;
use crate::provider::{HoloProviderCodecSpec, HoloStoreTableProvider};

/// Binary prefix that marks custom-encoded HoloFusion table providers.
const PROVIDER_MAGIC: &[u8] = b"holo_fusion.table_provider.v1:";

/// Logical extension codec that delegates to Ballista and adds provider support.
#[derive(Debug)]
pub struct HoloFusionLogicalExtensionCodec {
    /// Delegate for standard Ballista/DataFusion extension handling.
    inner: BallistaLogicalExtensionCodec,
    /// Shared metrics recorder passed to reconstructed table providers.
    metrics: Arc<PushdownMetrics>,
}

impl HoloFusionLogicalExtensionCodec {
    /// Builds a new codec instance with shared pushdown metrics.
    pub fn new(metrics: Arc<PushdownMetrics>) -> Self {
        Self {
            inner: BallistaLogicalExtensionCodec::default(),
            metrics,
        }
    }
}

impl LogicalExtensionCodec for HoloFusionLogicalExtensionCodec {
    /// Delegates logical extension decoding to the Ballista default codec.
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> DFResult<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    /// Delegates logical extension encoding to the Ballista default codec.
    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> DFResult<()> {
        self.inner.try_encode(node, buf)
    }

    /// Decodes table providers, handling HoloFusion-encoded providers first.
    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &SessionContext,
    ) -> DFResult<Arc<dyn TableProvider>> {
        // Decision: decode with HoloFusion-specific logic only when the payload
        // carries our magic prefix; otherwise delegate to Ballista defaults.
        // Decision: evaluate `if let Some(payload) = buf.strip_prefix(PROVIDER_MAGIC) {` to choose the correct SQL/storage control path.
        if let Some(payload) = buf.strip_prefix(PROVIDER_MAGIC) {
            let spec: HoloProviderCodecSpec = serde_json::from_slice(payload).map_err(|err| {
                DataFusionError::Internal(format!(
                    "failed to decode holo_fusion table provider for {table_ref}: {err}"
                ))
            })?;
            let provider = HoloStoreTableProvider::from_codec_spec(spec, self.metrics.clone())
                .map_err(|err| {
                    DataFusionError::Internal(format!(
                        "failed to build holo_fusion provider for {table_ref}: {err}"
                    ))
                })?;
            // Decision: reject mismatched schemas early so distributed execution
            // cannot run with a provider whose shape differs from the plan.
            // Decision: evaluate `if !provider` to choose the correct SQL/storage control path.
            if !provider
                .schema()
                .logically_equivalent_names_and_types(&schema)
                .is_ok()
            {
                return Err(DataFusionError::Internal(format!(
                    "schema mismatch when decoding provider for {table_ref}"
                )));
            }
            return Ok(Arc::new(provider));
        }

        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    /// Encodes table providers, handling `HoloStoreTableProvider` specially.
    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        // Decision: encode directly only when the provider is our concrete
        // type; all other providers continue through Ballista's codec.
        // Decision: evaluate `if let Some(provider) = node.as_any().downcast_ref::<HoloStoreTableProvider>() {` to choose the correct SQL/storage control path.
        if let Some(provider) = node.as_any().downcast_ref::<HoloStoreTableProvider>() {
            let payload = serde_json::to_vec(&provider.to_codec_spec())
                .with_context(|| format!("encode holo_fusion provider for {table_ref}"))
                .map_err(|err| DataFusionError::Internal(err.to_string()))?;
            buf.extend_from_slice(PROVIDER_MAGIC);
            buf.extend_from_slice(payload.as_slice());
            return Ok(());
        }

        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    /// Delegates file format decoding to the Ballista default codec.
    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &SessionContext,
    ) -> DFResult<Arc<dyn FileFormatFactory>> {
        self.inner.try_decode_file_format(buf, ctx)
    }

    /// Delegates file format encoding to the Ballista default codec.
    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> DFResult<()> {
        self.inner.try_encode_file_format(buf, node)
    }

    /// Delegates scalar UDF decoding to the Ballista default codec.
    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> DFResult<Arc<ScalarUDF>> {
        self.inner.try_decode_udf(name, buf)
    }

    /// Delegates scalar UDF encoding to the Ballista default codec.
    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> DFResult<()> {
        self.inner.try_encode_udf(node, buf)
    }

    /// Delegates aggregate UDF decoding to the Ballista default codec.
    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> DFResult<Arc<AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    /// Delegates aggregate UDF encoding to the Ballista default codec.
    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> DFResult<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    /// Delegates window UDF decoding to the Ballista default codec.
    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> DFResult<Arc<WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    /// Delegates window UDF encoding to the Ballista default codec.
    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> DFResult<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}
