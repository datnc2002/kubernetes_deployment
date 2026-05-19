# Sử dụng image node js chính thức làm base image
FROM node:18-alpine

# Thiết lập thư mục làm việc trong container
WORKDIR /usr/src/app

# Sao chép package.json và package-lock.json (nếu có)
COPY package*.json ./

# Cài đặt các dependencies
RUN npm install

# Sao chép toàn bộ mã nguồn vào container
COPY . .

# Expose port ứng dụng sẽ chạy
EXPOSE 3000

# Lệnh khởi chạy ứng dụng
CMD [ "npm", "start" ]
