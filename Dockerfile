FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o consumer .

# Create a minimal image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/consumer /app/
COPY --from=builder /app/config /app/config

# Make sure the binary is executable
RUN chmod +x /app/consumer

# Run the application
CMD ["/app/consumer"] 