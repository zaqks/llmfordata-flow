const loader = document.getElementById('loader');

fetch('/anal-url')
  .then(response => response.json())
  .then(data => {
    if (data.url) {
      const iframe = document.createElement('iframe');
      iframe.src = data.url;
      
      // Hide loader when iframe loads
      iframe.onload = () => {
        loader.classList.add('hidden');
      };
      
      document.getElementById('hot-iframe-container').appendChild(iframe);
    } else {
        // If no URL, maybe hide loader or show error?
        // For now, let's just hide it so it doesn't spin forever
        loader.classList.add('hidden');
    }
  })
  .catch(error => {
      console.error('Error fetching host URL:', error);
      loader.classList.add('hidden');
  });
