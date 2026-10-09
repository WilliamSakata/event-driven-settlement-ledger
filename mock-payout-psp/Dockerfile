FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

EXPOSE 4003

CMD ["npx", "tsx", "server.ts"]
