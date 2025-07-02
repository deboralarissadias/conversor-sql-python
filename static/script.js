document.addEventListener('DOMContentLoaded', function() {
    // Flag to track if a conversion is in progress
    let isConverting = false;
    
    // Tab switching
    const tabs = document.querySelectorAll('.tab');
    const inputContainers = document.querySelectorAll('.input-container');
    
    tabs.forEach(tab => {
        tab.addEventListener('click', function() {
            // Remove active class from all tabs
            tabs.forEach(t => t.classList.remove('active'));
            // Add active class to clicked tab
            this.classList.add('active');
            
            // Hide all input containers
            inputContainers.forEach(container => container.classList.remove('active'));
            // Show the corresponding container
            document.getElementById(this.dataset.target).classList.add('active');
        });
    });
    
    // Convert button click handler
    document.getElementById('convert-btn').addEventListener('click', function() {
        // Prevent multiple clicks while processing
        if (isConverting) {
            return;
        }
        
        const activeTab = document.querySelector('.tab.active');
        let sqlQuery = '';
        
        if (activeTab.dataset.target === 'text-input') {
            sqlQuery = document.getElementById('sql-textarea').value.trim();
            if (!sqlQuery) {
                showError('Please enter a SQL query.');
                return;
            }
            convertSql(sqlQuery);
        } else {
            const fileInput = document.getElementById('sql-file');
            if (!fileInput.files || fileInput.files.length === 0) {
                showError('Please select a SQL file.');
                return;
            }
            
            const file = fileInput.files[0];
            const reader = new FileReader();
            
            reader.onload = function(e) {
                sqlQuery = e.target.result;
                convertSql(sqlQuery);
            };
            
            reader.onerror = function() {
                showError('Error reading the file.');
                isConverting = false;
            };
            
            reader.readAsText(file);
        }
    });
    
    // Function to convert SQL
    function convertSql(sqlQuery) {
        isConverting = true;
        showLoader();
        hideError();
        
        // Display original SQL immediately
        document.getElementById('original-sql').textContent = sqlQuery;
        
        // Call the backend to convert SQL
        fetch('/convert', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query: sqlQuery }),
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            // Format and display the PySpark code
            document.getElementById('pyspark-output').textContent = data.pyspark;
            
            // Format and display the SparkSQL code
            document.getElementById('sparksql-output').textContent = data.sparksql;
            
            // Show output section
            document.getElementById('output-section').style.display = 'block';
            hideLoader();
            
            // Apply syntax highlighting if available
            if (window.hljs) {
                document.querySelectorAll('pre code').forEach((block) => {
                    hljs.highlightElement(block);
                });
            }
            
            // Highlight the PySpark tab after conversion
            document.querySelector('.output-tab[data-target="pyspark-container"]').click();
            
            // Reset conversion flag
            isConverting = false;
        })
        .catch(error => {
            console.error('Error:', error);
            showError('Error converting SQL: ' + error.message);
            hideLoader();
            isConverting = false;
        });
    }
    
    // Helper functions
    function showLoader() {
        document.getElementById('loader').style.display = 'block';
    }
    
    function hideLoader() {
        document.getElementById('loader').style.display = 'none';
    }
    
    function showError(message) {
        const errorElement = document.getElementById('error-message');
        errorElement.textContent = message;
        errorElement.style.display = 'block';
    }
    
    function hideError() {
        document.getElementById('error-message').style.display = 'none';
    }
    
    // Output tab switching
    const outputTabs = document.querySelectorAll('.output-tab');
    const outputContainers = document.querySelectorAll('.output-container');
    
    outputTabs.forEach(tab => {
        tab.addEventListener('click', function() {
            // Remove active class from all tabs
            outputTabs.forEach(t => t.classList.remove('active'));
            // Add active class to clicked tab
            this.classList.add('active');
            
            // Hide all output containers
            outputContainers.forEach(container => container.classList.remove('active'));
            // Show the corresponding container
            document.getElementById(this.dataset.target).classList.add('active');
        });
    });
    
    // Initialize syntax highlighting
    if (window.hljs) {
        hljs.highlightAll();
    }
});