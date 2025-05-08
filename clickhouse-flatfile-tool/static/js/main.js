document.addEventListener('DOMContentLoaded', function() {
    // Global variables to store state
    let appState = {
        sourceType: 'clickhouse', // 'clickhouse' or 'flatfile'
        connectionConfig: {},
        selectedTable: '',
        selectedColumns: [],
        fileData: null,
        joinEnabled: false,
        joinConfig: {
            base_table: '',
            join_tables: [],
            join_conditions: []
        }
    };
    
    // DOM references
    const ingestionDirectionRadios = document.querySelectorAll('input[name="ingestionDirection"]');
    const connectClickhouseBtn = document.getElementById('connectClickhouse');
    const tableSelect = document.getElementById('chTable');
    const enableJoinCheckbox = document.getElementById('enableJoin');
    const joinConfigSection = document.getElementById('joinConfiguration');
    const uploadFlatFileBtn = document.getElementById('uploadFlatFile');
    const selectAllColumnsBtn = document.getElementById('selectAllColumns');
    const deselectAllColumnsBtn = document.getElementById('deselectAllColumns');
    const previewDataBtn = document.getElementById('previewData');
    const startIngestionBtn = document.getElementById('startIngestion');
    
    // Toggle sections based on ingestion direction
    for (const radio of ingestionDirectionRadios) {
        radio.addEventListener('change', function() {
            if (this.value === 'clickhouse-to-flatfile') {
                appState.sourceType = 'clickhouse';
                document.querySelector('.ch-source-title').textContent = 'ClickHouse Source';
                document.querySelector('.ff-target-title').textContent = 'Flat File Target';
                document.getElementById('flatFileSource').classList.add('d-none');
                document.getElementById('flatFileTarget').classList.remove('d-none');
                document.getElementById('clickhouseTarget').classList.add('d-none');
                document.getElementById('joinTableSection').classList.remove('d-none');
                // Reset UI state
                resetColumnSelection();
                resetPreview();
            } else {
                appState.sourceType = 'flatfile';
                document.querySelector('.ch-source-title').textContent = 'ClickHouse Target';
                document.querySelector('.ff-target-title').textContent = 'Flat File Source';
                document.getElementById('flatFileSource').classList.remove('d-none');
                document.getElementById('flatFileTarget').classList.add('d-none');
                document.getElementById('clickhouseTarget').classList.remove('d-none');
                document.getElementById('joinTableSection').classList.add('d-none');
                // Reset UI state
                resetColumnSelection();  
                resetPreview();
            }
        });
    }
    
    // Connect to ClickHouse
    connectClickhouseBtn.addEventListener('click', function() {
        // Get connection parameters
        const config = {
            host: document.getElementById('chHost').value || 'localhost',
            port: document.getElementById('chPort').value || '9000',
            database: document.getElementById('chDatabase').value || 'default',
            user: document.getElementById('chUser').value || 'default',
            jwt_token: document.getElementById('chJwtToken').value || null
        };
        
        // Update status
        updateStatus('connecting', 'Connecting to ClickHouse...', 10);
        
        // Make API request
        fetch('/api/connect/clickhouse', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(config)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Store connection config
                appState.connectionConfig = config;
                
                // Update tables dropdown
                populateTablesDropdown(data.tables);
                
                // Show table selection
                document.getElementById('tableSelection').classList.remove('d-none');
                
                // Update status
                updateStatus('success', 'Connected to ClickHouse successfully', 100);
                
                // Enable join section if in ClickHouse to Flat File mode
                if (appState.sourceType === 'clickhouse') {
                    document.getElementById('joinTableSection').classList.remove('d-none');
                }
            } else {
                throw new Error(data.message);
            }
        })
        .catch(error => {
            console.error('Connection error:', error);
            updateStatus('error', `Connection failed: ${error.message}`, 0);
        });
    });
    
    // Handle table selection
    tableSelect.addEventListener('change', function() {
        const selectedTable = this.value;
        if (selectedTable) {
            appState.selectedTable = selectedTable;
            appState.joinConfig.base_table = selectedTable;
            
            // Fetch columns for the selected table
            fetchTableColumns(selectedTable);
        }
    });
    
    // Handle enable join checkbox
    enableJoinCheckbox.addEventListener('change', function() {
        if (this.checked) {
            joinConfigSection.classList.remove('d-none');
            appState.joinEnabled = true;
            
            // Populate join tables (exclude selected base table)
            populateJoinTables();
        } else {
            joinConfigSection.classList.add('d-none');
            appState.joinEnabled = false;
        }
    });
    
    // Handle join tables selection
    document.getElementById('joinTables').addEventListener('change', function() {
        const selectedTables = Array.from(this.selectedOptions).map(option => option.value);
        appState.joinConfig.join_tables = selectedTables;
    });
    
    // Handle join conditions input
    document.getElementById('joinConditions').addEventListener('input', function() {
        const conditionsText = this.value;
        // Simple parsing for conditions (in real app would need more robust parsing)
        appState.joinConfig.join_conditions = [conditionsText];
    });
    
    // Upload and parse flat file
    uploadFlatFileBtn.addEventListener('click', function() {
        const fileInput = document.getElementById('inputFile');
        const file = fileInput.files[0];
        
        if (!file) {
            alert('Please select a file to upload');
            return;
        }
        
        const delimiter = document.getElementById('inputDelimiter').value;
        
        // Create FormData for file upload
        const formData = new FormData();
        formData.append('file', file);
        formData.append('delimiter', delimiter);
        
        // Update status
        updateStatus('connecting', 'Uploading and parsing file...', 20);
        
        // Upload file
        fetch('/api/upload/flatfile', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Store file data
                appState.fileData = {
                    filename: data.filename,
                    filepath: data.filepath,
                    delimiter: delimiter
                };
                
                // Display columns
                populateColumnsFromFlatFile(data.columns);
                
                // Update status
                updateStatus('success', 'File uploaded and parsed successfully', 100);
                
                // Enable buttons
                previewDataBtn.disabled = false;
            } else {
                throw new Error(data.message);
            }
        })
        .catch(error => {
            console.error('File upload error:', error);
            updateStatus('error', `File upload failed: ${error.message}`, 0);
        });
    });
    
    // Select/Deselect all columns
    selectAllColumnsBtn.addEventListener('click', function() {
        const checkboxes = document.querySelectorAll('#columnList input[type="checkbox"]');
        checkboxes.forEach(checkbox => {
            checkbox.checked = true;
            
            // Update selected columns array
            const columnName = checkbox.value;
            if (!appState.selectedColumns.includes(columnName)) {
                appState.selectedColumns.push(columnName);
            }
        });
        
        updateColumnCount();
        previewDataBtn.disabled = appState.selectedColumns.length === 0;
        startIngestionBtn.disabled = appState.selectedColumns.length === 0;
    });
    
    deselectAllColumnsBtn.addEventListener('click', function() {
        const checkboxes = document.querySelectorAll('#columnList input[type="checkbox"]');
        checkboxes.forEach(checkbox => {
            checkbox.checked = false;
        });
        
        appState.selectedColumns = [];
        updateColumnCount();
        previewDataBtn.disabled = true;
        startIngestionBtn.disabled = true;
    });
    
    // Preview data button
    previewDataBtn.addEventListener('click', function() {
        if (appState.sourceType === 'clickhouse') {
            previewClickHouseData();
        } else {
            previewFlatFileData();
        }
    });
    
    // Start ingestion button
    startIngestionBtn.addEventListener('click', function() {
        if (appState.sourceType === 'clickhouse') {
            startClickHouseToFlatFileIngestion();
        } else {
            startFlatFileToClickHouseIngestion();
        }
    });
    
    // Helper functions
    function fetchTableColumns(tableName) {
        // Construct request data
        const requestData = {
            ...appState.connectionConfig,
            table: tableName
        };
        
        // Update status
        updateStatus('connecting', 'Fetching columns...', 50);
        
        // Fetch columns
        fetch('/api/get/table/columns', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Display columns
                populateColumnsFromClickHouse(data.columns);
                
                // Update status
                updateStatus('success', 'Columns fetched successfully', 100);
                
                // Enable buttons
                previewDataBtn.disabled = false;
            } else {
                throw new Error(data.message);
            }
        })
        .catch(error => {
            console.error('Error fetching columns:', error);
            updateStatus('error', `Failed to fetch columns: ${error.message}`, 0);
        });
    }
    
    function populateTablesDropdown(tables) {
        // Clear existing options (except the first one)
        while (tableSelect.options.length > 1) {
            tableSelect.options.remove(1);
        }
        
        // Add table options
        tables.forEach(table => {
            const option = document.createElement('option');
            option.value = table;
            option.textContent = table;
            tableSelect.appendChild(option);
        });
    }
    
    function populateJoinTables() {
        // Get all tables except the base table
        const joinTablesSelect = document.getElementById('joinTables');
        
        // First get all tables
        fetch('/api/connect/clickhouse', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(appState.connectionConfig)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Clear existing options
                joinTablesSelect.innerHTML = '';
                
                // Add options for all tables except base table
                data.tables.forEach(table => {
                    if (table !== appState.selectedTable) {
                        const option = document.createElement('option');
                        option.value = table;
                        option.textContent = table;
                        joinTablesSelect.appendChild(option);
                    }
                });
            } else {
                throw new Error(data.message);
            }
        })
        .catch(error => {
            console.error('Error populating join tables:', error);
        });
    }
    
    function populateColumnsFromClickHouse(columns) {
        const columnList = document.getElementById('columnList');
        columnList.innerHTML = '';
        appState.selectedColumns = [];
        
        columns.forEach(column => {
            const columnDiv = document.createElement('div');
            columnDiv.className = 'column-item';
            
            const checkboxContainer = document.createElement('div');
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `col-${column.name}`;
            checkbox.value = column.name;
            checkbox.className = 'form-check-input me-2';
            
            const label = document.createElement('label');
            label.htmlFor = `col-${column.name}`;
            label.className = 'form-check-label';
            label.textContent = column.name;
            
            checkboxContainer.appendChild(checkbox);
            checkboxContainer.appendChild(label);
            
            const typeSpan = document.createElement('span');
            typeSpan.className = 'column-type';
            typeSpan.textContent = column.type;
            
            columnDiv.appendChild(checkboxContainer);
            columnDiv.appendChild(typeSpan);
            
            columnList.appendChild(columnDiv);
            
            // Add event listener to checkbox
            checkbox.addEventListener('change', function() {
                if (this.checked) {
                    appState.selectedColumns.push(column.name);
                } else {
                    const index = appState.selectedColumns.indexOf(column.name);
                    if (index !== -1) {
                        appState.selectedColumns.splice(index, 1);
                    }
                }
                
                updateColumnCount();
                previewDataBtn.disabled = appState.selectedColumns.length === 0;
                startIngestionBtn.disabled = appState.selectedColumns.length === 0;
            });
        });
        
        // Show column selection section
        document.getElementById('columnSelectionWrapper').classList.remove('d-none');
    }
    
    function populateColumnsFromFlatFile(columns) {
        const columnList = document.getElementById('columnList');
        columnList.innerHTML = '';
        appState.selectedColumns = [];
        
        columns.forEach(column => {
            const columnDiv = document.createElement('div');
            columnDiv.className = 'column-item';
            
            const checkboxContainer = document.createElement('div');
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `col-${column.name}`;
            checkbox.value = column.name;
            checkbox.className = 'form-check-input me-2';
            
            const label = document.createElement('label');
            label.htmlFor = `col-${column.name}`;
            label.className = 'form-check-label';
            label.textContent = column.name;
            
            checkboxContainer.appendChild(checkbox);
            checkboxContainer.appendChild(label);
            
            const typeSpan = document.createElement('span');
            typeSpan.className = 'column-type';
            typeSpan.textContent = column.type || 'String'; // Default to String if type not provided
            
            columnDiv.appendChild(checkboxContainer);
            columnDiv.appendChild(typeSpan);
            
            columnList.appendChild(columnDiv);
            
            // Add event listener to checkbox
            checkbox.addEventListener('change', function() {
                if (this.checked) {
                    appState.selectedColumns.push(column.name);
                } else {
                    const index = appState.selectedColumns.indexOf(column.name);
                    if (index !== -1) {
                        appState.selectedColumns.splice(index, 1);
                    }
                }
                
                updateColumnCount();
                previewDataBtn.disabled = appState.selectedColumns.length === 0;
                startIngestionBtn.disabled = appState.selectedColumns.length === 0;
            });
        });
        
        // Show column selection section
        document.getElementById('columnSelectionWrapper').classList.remove('d-none');
    }
    
    function updateColumnCount() {
        const countElement = document.getElementById('selectedColumnsCount');
        countElement.textContent = appState.selectedColumns.length;
    }
    
    function resetColumnSelection() {
        document.getElementById('columnList').innerHTML = '';
        appState.selectedColumns = [];
        updateColumnCount();
        document.getElementById('columnSelectionWrapper').classList.add('d-none');
        previewDataBtn.disabled = true;
        startIngestionBtn.disabled = true;
    }
    
    function resetPreview() {
        const previewTable = document.getElementById('previewTable');
        if (previewTable) {
            previewTable.innerHTML = '';
        }
        
        document.getElementById('previewSection').classList.add('d-none');
        document.getElementById('previewStatus').textContent = '';
    }
    
    function updateStatus(status, message, progress) {
        const statusDiv = document.getElementById('statusMessage');
        const progressBar = document.getElementById('statusProgress');
        
        if (status === 'connecting') {
            statusDiv.className = 'alert alert-info';
        } else if (status === 'success') {
            statusDiv.className = 'alert alert-success';
        } else if (status === 'error') {
            statusDiv.className = 'alert alert-danger';
        } else if (status === 'warning') {
            statusDiv.className = 'alert alert-warning';
        }
        
        statusDiv.textContent = message;
        progressBar.style.width = `${progress}%`;
        progressBar.setAttribute('aria-valuenow', progress);
    }
    
    function previewClickHouseData() {
        // Construct request data
        const requestData = {
            ...appState.connectionConfig,
            table: appState.selectedTable,
            columns: appState.selectedColumns,
            limit: 10 // Show top 10 rows for preview
        };
        
        // Add join config if join is enabled
        if (appState.joinEnabled) {
            requestData.join = appState.joinConfig;
        }
        
        // Update status
        updateStatus('connecting', 'Fetching preview data...', 50);
        
        // Fetch preview data
        fetch('/api/preview/clickhouse', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Display preview
                displayPreviewTable(data.data);
                
                // Update status
                updateStatus('success', 'Preview data fetched successfully', 100);
                
                // Enable start ingestion button
                startIngestionBtn.disabled = false;
            } else {
                throw new Error(data.message);
            }
        })
        .catch(error => {
            console.error('Error fetching preview data:', error);
            updateStatus('error', `Failed to fetch preview data: ${error.message}`, 0);
        });
    }
    
    function previewFlatFileData() {
        // Construct request data
        const requestData = {
            filepath: appState.fileData.filepath,
            delimiter: appState.fileData.delimiter,
            columns: appState.selectedColumns,
            limit: 10 // Show top 10 rows for preview
        };
        
        // Update status
        updateStatus('connecting', 'Generating preview...', 50);
        
        // Fetch preview data
        fetch('/api/preview/flatfile', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Display preview
                displayPreviewTable(data.data);
                
                // Update status
                updateStatus('success', 'Preview generated successfully', 100);
                
                // Enable start ingestion button
                startIngestionBtn.disabled = false;
            } else {
                throw new Error(data.message);
            }
        })
        .catch(error => {
            console.error('Error generating preview:', error);
            updateStatus('error', `Failed to generate preview: ${error.message}`, 0);
        });
    }
    
    function displayPreviewTable(data) {
        const previewSection = document.getElementById('previewSection');
        const previewTable = document.getElementById('previewTable');
        const previewStatus = document.getElementById('previewStatus');
        
        // Clear existing table
        previewTable.innerHTML = '';
        
        if (data.length === 0) {
            previewStatus.textContent = 'No data to preview';
            return;
        }
        
        // Create table header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        
        Object.keys(data[0]).forEach(key => {
            const th = document.createElement('th');
            th.textContent = key;
            headerRow.appendChild(th);
        });
        
        thead.appendChild(headerRow);
        previewTable.appendChild(thead);
        
        // Create table body
        const tbody = document.createElement('tbody');
        
        data.forEach(row => {
            const tr = document.createElement('tr');
            
            Object.values(row).forEach(value => {
                const td = document.createElement('td');
                td.textContent = value !== null && value !== undefined ? value : 'NULL';
                tr.appendChild(td);
            });
            
            tbody.appendChild(tr);
        });
        
        previewTable.appendChild(tbody);
        
        // Show preview section
        previewSection.classList.remove('d-none');
        previewStatus.textContent = `Showing ${data.length} rows`;
    }
    
    function startClickHouseToFlatFileIngestion() {
        // Get output filename and format
        const outputFilename = document.getElementById('outputFilename').value;
        const outputFormat = document.getElementById('outputFormat').value;
        
        if (!outputFilename) {
            alert('Please enter an output filename');
            return;
        }
        
        // Construct request data
        const requestData = {
            ...appState.connectionConfig,
            table: appState.selectedTable,
            columns: appState.selectedColumns,
            outputFilename: outputFilename,
            outputFormat: outputFormat
        };
        
        // Add join config if join is enabled
        if (appState.joinEnabled) {
            requestData.join = appState.joinConfig;
        }
        
        // Update status
        updateStatus('connecting', 'Starting data ingestion...', 10);
        
        // Start ingestion
        fetch('/api/ingest/clickhouse-to-flatfile', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Show success message with download link
                updateStatus('success', `Ingestion completed successfully. ${data.rowCount} rows exported.`, 100);
                
                // Add download link
                if (data.downloadUrl) {
                    const downloadLink = document.createElement('a');
                    downloadLink.href = data.downloadUrl;
                    downloadLink.textContent = 'Download File';
                    downloadLink.className = 'btn btn-primary mt-3';
                    downloadLink.download = outputFilename;
                    
                    const statusDiv = document.getElementById('statusMessage');
                    statusDiv.appendChild(document.createElement('br'));
                    statusDiv.appendChild(downloadLink);
                }
            } else {
                throw new Error(data.message);
            }
        })
        .catch(error => {
            console.error('Ingestion error:', error);
            updateStatus('error', `Ingestion failed: ${error.message}`, 0);
        });
    }
    
    function startFlatFileToClickHouseIngestion() {
        // Get target table name
        const targetTable = document.getElementById('targetTable').value;
        
        if (!targetTable) {
            alert('Please enter a target table name');
            return;
        }
        
        // Construct request data
        const requestData = {
            ...appState.connectionConfig,
            sourceFilepath: appState.fileData.filepath,
            sourceDelimiter: appState.fileData.delimiter,
            columns: appState.selectedColumns,
            targetTable: targetTable,
            createIfNotExists: document.getElementById('createTableIfNotExists').checked
        };
        
        // Update status
        updateStatus('connecting', 'Starting data ingestion...', 10);
        
        // Start ingestion
        fetch('/api/ingest/flatfile-to-clickhouse', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Show success message
                updateStatus('success', `Ingestion completed successfully. ${data.rowCount} rows imported.`, 100);
            } else {
                throw new Error(data.message);
            }
        })
        .catch(error => {
            console.error('Ingestion error:', error);
            updateStatus('error', `Ingestion failed: ${error.message}`, 0);
        });
    }
});