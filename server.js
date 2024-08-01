const express = require('express');
const path = require('path');
const http = require('http');

function returnSimpleText(req, res, next) {
    res.send(`This is just a stub site. This message is generated at [${Date.now()}]`);
}

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', returnSimpleText);
app.get('/', returnSimpleText);
app.use((req, res /* , next */) => {
  res.redirect('/');
});

//const port = process.env.PORT || 3000;
//app.listen(port, () => {
//  console.log('Listening on %d.', port);
//});
const server = http.createServer(app);
server.listen(process.env.PORT || '3000', () => {
  console.log('Listening on %d.', server.address().port);
});
