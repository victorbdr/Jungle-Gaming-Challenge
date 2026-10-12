FROM oven/bun:1.4 AS base
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production
COPY tsconfig.json ./
COPY src ./src
EXPOSE 3000
STOPSIGNAL SIGTERM
CMD ["bun", "src/main.ts"]
