FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

EXPOSE 3003

CMD ["npx", "tsx", "src/main.ts"]
