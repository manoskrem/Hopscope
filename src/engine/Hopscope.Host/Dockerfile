# syntax=docker/dockerfile:1
# Build context: repo root

# ---- build ----
FROM mcr.microsoft.com/dotnet/sdk:10.0-noble-aot AS build
ARG TARGETARCH
WORKDIR /src

# arm64-only: Grpc.Tools' bundled linux_arm64 protoc SIGSEGVs (exit 139 → MSB6006) when
# MSBuild's ProtoCompile task spawns it under Docker Desktop on Apple Silicon — the same
# binary runs fine standalone, so it's the MSBuild spawn that trips this specific build.
# The system protoc does not crash, so we install it and point Grpc.Tools at it below via
# Protobuf_ProtocFullPath (the bundled grpc_csharp_plugin still does the --grpc_out codegen).
# amd64 is untouched (its bundled protoc works), so the CI image build is byte-for-byte the same.
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        apt-get update \
        && apt-get install -y --no-install-recommends protobuf-compiler \
        && rm -rf /var/lib/apt/lists/*; \
    fi

# Copy solution + all .csproj files first so this layer is cached on source-only changes.
COPY ["Hopscope.sln", "./"]
COPY ["src/engine/Hopscope.Domain/Hopscope.Domain.csproj",           "src/engine/Hopscope.Domain/"]
COPY ["src/engine/Hopscope.Application/Hopscope.Application.csproj", "src/engine/Hopscope.Application/"]
COPY ["src/engine/Hopscope.Infrastructure/Hopscope.Infrastructure.csproj", "src/engine/Hopscope.Infrastructure/"]
COPY ["src/engine/Hopscope.Push/Hopscope.Push.csproj",               "src/engine/Hopscope.Push/"]
COPY ["src/engine/Hopscope.Host/Hopscope.Host.csproj",               "src/engine/Hopscope.Host/"]

RUN dotnet restore "src/engine/Hopscope.Host/Hopscope.Host.csproj" -a $TARGETARCH

# Copy the rest of the source and publish.
# On arm64 override protoc with the system one (installed above); amd64 uses the bundled protoc.
COPY . .
RUN PROTOC_OVERRIDE=""; \
    if [ "$TARGETARCH" = "arm64" ]; then PROTOC_OVERRIDE="/p:Protobuf_ProtocFullPath=/usr/bin/protoc"; fi; \
    dotnet publish "src/engine/Hopscope.Host/Hopscope.Host.csproj" \
    -c Release -a $TARGETARCH --no-restore -o /app \
    /p:PublishAot=true /p:TreatWarningsAsErrors=true $PROTOC_OVERRIDE

# ---- runtime: native binary, no CoreCLR ----
# .NET 10 folded the AOT-tuned runtime-deps into the standard chiseled image (the old
# `-chiseled-aot` runtime tag was retired); InvariantGlobalization=true drops ICU/tzdata.
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS final
WORKDIR /app
# Default build is Kafka-free → the single self-contained Native-AOT binary is all that
# ships (no native libraries to carry). The Kafka opt-in variant has its own Dockerfile.kafka
# which copies the whole publish dir (to carry native librdkafka) onto a non-chiseled base.
COPY --from=build /app/Hopscope.Host ./Hopscope.Host
# 8080 = HTTP/1.1 (WebSocket + REST control-plane)
# 4317 = HTTP/2 h2c (OTLP gRPC receiver, enabled via HOPSCOPE_OTLP_ENABLED=true)
EXPOSE 8080
EXPOSE 4317
USER $APP_UID
ENTRYPOINT ["./Hopscope.Host"]
