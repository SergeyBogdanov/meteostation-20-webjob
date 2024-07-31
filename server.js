const express = require('express');
const http = require('http');

function returnSimpleText(req, res, next) {
    res.send(`This is just a stub site. This message is generated at [${Date.now()}]`);
}

// Redirect requests to the public subdirectory to the root
const app = express();
app.get('/', returnSimpleText);
app.use((req, res /* , next */) => {
  res.redirect('/');
});

const server = http.createServer(app);
server.listen(process.env.PORT || '3000', () => {
  console.log('Listening on %d.', server.address().port);
});
