const express = require("express");
const path = require("path");
const fs = require("fs");
const app = express();
const PORT = process.env.PORT || 3001;

const publicDir = path.join(__dirname, "public");

// Serve static files from the public directory
app.use(express.static(publicDir));

// Serve the index.html file for the root route
app.get("/", (req, res) => {
  res.sendFile(path.join(publicDir, "index.html"));
});


// Start the server
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
  console.log(`View the knowledge graph at http://localhost:${PORT}`);
});
