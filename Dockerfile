FROM apify/actor-node:18
 
# Copy package files first for layer caching
COPY package*.json ./
 
# Install production dependencies only
RUN npm --quiet set progress=false \
 && npm install --only=prod --no-optional \
 && echo "Dependencies installed"
 
# Copy actor source
COPY . ./
 
CMD npm start --silent
