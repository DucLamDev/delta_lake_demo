FROM node:16-slim

# Thiết lập thư mục làm việc cho ứng dụng frontend
WORKDIR /app

# Sao chép các file npm cần thiết
COPY package.json package-lock.json ./

# Cài đặt dependencies
RUN npm install

# Sao chép toàn bộ mã nguồn vào container
COPY . /app

# Build ứng dụng Next.js
RUN npm run build

# Chạy ứng dụng
CMD ["npm", "start"]
