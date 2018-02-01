#
# ---- Base Node ----
FROM alpine:3.7 AS base
# install node
RUN apk add --no-cache nodejs
# set working directory
WORKDIR /root/mongo-stream
# copy project file
COPY package.json .

#
# ---- Dependencies ----
FROM base AS dependencies
# install node_modules
RUN npm set progress=false && npm config set depth 0
RUN npm install --only=production
# copy production node_modules aside
RUN cp -R node_modules prod_node_modules
# install ALL node_modules, including 'devDependencies'
RUN npm install

#
# ---- Release ----
FROM base AS release
# copy production node_modules
COPY --from=dependencies /root/mongo-stream/prod_node_modules ./node_modules
# copy app sources
COPY . .
# expose port and define CMD
EXPOSE 3000
CMD npm run start
