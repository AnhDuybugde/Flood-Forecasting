const http = require('http');

http.get('http://localhost:8000/eda.html', (res) => {
  let data = '';
  res.on('data', (chunk) => {
    data += chunk;
  });
  res.on('end', () => {
    console.log("Response length:", data.length);
  });
}).on('error', (err) => {
  console.log("Error: " + err.message);
});
