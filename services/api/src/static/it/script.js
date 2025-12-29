fetch('/host-url')
  .then(response => response.json())
  .then(data => {
    if (data.url) {
      const iframe = document.createElement('iframe');
      iframe.src = data.url;
      document.getElementById('hot-iframe-container').appendChild(iframe);
    }
  });
