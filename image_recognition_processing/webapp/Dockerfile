FROM node:12.13.0-alpine AS build
RUN mkdir /build
COPY . /build
WORKDIR /build
RUN npm i
RUN npm run tsc

FROM nginx:1.16.1-alpine
COPY --from=build /build/ /usr/share/nginx/html
COPY entrypoint.sh /bin
RUN chmod 755 /bin/entrypoint.sh
ENTRYPOINT [ "/bin/entrypoint.sh" ]
CMD ["nginx", "-g", "daemon off;"]
