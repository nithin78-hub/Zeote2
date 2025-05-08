import { useState, useEffect } from 'react';
import { ArrowDown, ArrowUp, Database, FileText, RefreshCw, Check, AlertTriangle, BarChart2, Cpu, Clock, FileType } from 'lucide-react';

// Mock data for demonstration
const mockStats = {
  totalTransfers: 42,
  activeTransfers: 3,
  completedTransfers: 39,
  errorTransfers: 0,
  totalRows: 1287654,
  avgTransferTime: "2m 34s"
};

const mockTransfers = [
  { 
    id: 1, 
    source: "users_table", 
    target: "users_export.csv", 
    direction: "ch-to-ff", 
    status: "completed", 
    progress: 100, 
    rows: 10250, 
    startTime: "2025-05-08T08:45:12", 
    endTime: "2025-05-08T08:47:38"
  },
  { 
    id: 2, 
    source: "orders_table", 
    target: "orders_export.csv", 
    direction: "ch-to-ff", 
    status: "completed", 
    progress: 100, 
    rows: 50430, 
    startTime: "2025-05-08T09:15:22", 
    endTime: "2025-05-08T09:20:17"
  },
  { 
    id: 3, 
    source: "products_import.csv", 
    target: "products", 
    direction: "ff-to-ch", 
    status: "in-progress", 
    progress: 67, 
    rows: 5280, 
    startTime: "2025-05-08T10:02:11", 
    endTime: null
  },
  { 
    id: 4, 
    source: "customers.csv", 
    target: "customers", 
    direction: "ff-to-ch", 
    status: "in-progress", 
    progress: 48, 
    rows: 8700, 
    startTime: "2025-05-08T10:05:32", 
    endTime: null
  },
  { 
    id: 5, 
    source: "transactions_table", 
    target: "transactions.csv", 
    direction: "ch-to-ff", 
    status: "in-progress", 
    progress: 24, 
    rows: 87210, 
    startTime: "2025-05-08T10:10:01", 
    endTime: null
  }
];

// Dashboard component
export default function DataIngestionDashboard() {
  const [stats, setStats] = useState(mockStats);
  const [transfers, setTransfers] = useState(mockTransfers);
  const [refreshing, setRefreshing] = useState(false);

  // Simulating refresh action
  const handleRefresh = () => {
    setRefreshing(true);
    setTimeout(() => {
      setRefreshing(false);
    }, 1000);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-blue-600 text-white p-4 shadow-md">
        <div className="container mx-auto flex justify-between items-center">
          <h1 className="text-2xl font-bold">ClickHouse & Flat File Data Ingestion Dashboard</h1>
          <button 
            className="bg-blue-700 hover:bg-blue-800 px-4 py-2 rounded flex items-center gap-2 transition" 
            onClick={handleRefresh}
          >
            <RefreshCw size={18} className={refreshing ? "animate-spin" : ""} />
            Refresh
          </button>
        </div>
      </header>

      <main className="container mx-auto py-6 px-4">
        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
          <StatsCard 
            title="Total Transfers" 
            value={stats.totalTransfers} 
            icon={<FileType className="text-blue-500" />} 
          />
          <StatsCard 
            title="Active Transfers" 
            value={stats.activeTransfers} 
            icon={<RefreshCw className="text-yellow-500" />} 
          />
          <StatsCard 
            title="Completed Transfers" 
            value={stats.completedTransfers} 
            icon={<Check className="text-green-500" />} 
          />
          <StatsCard 
            title="Transfer Errors" 
            value={stats.errorTransfers} 
            icon={<AlertTriangle className="text-red-500" />} 
          />
          <StatsCard 
            title="Total Rows Transferred" 
            value={stats.totalRows.toLocaleString()} 
            icon={<BarChart2 className="text-purple-500" />} 
          />
          <StatsCard 
            title="Avg. Transfer Time" 
            value={stats.avgTransferTime} 
            icon={<Clock className="text-indigo-500" />} 
          />
        </div>

        {/* Transfer Activity */}
        <div className="bg-white rounded-lg shadow mb-6">
          <div className="border-b border-gray-200 p-4">
            <h2 className="text-lg font-medium">Recent Transfer Activity</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Source</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Target</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Direction</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Progress</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rows</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Started</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {transfers.map((transfer) => (
                  <tr key={transfer.id}>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{transfer.source}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{transfer.target}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      <DirectionBadge direction={transfer.direction} />
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <StatusBadge status={transfer.status} />
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      <div className="w-full bg-gray-200 rounded-full h-2.5">
                        <div 
                          className={`h-2.5 rounded-full ${transfer.status === 'completed' ? 'bg-green-500' : 'bg-blue-500'}`} 
                          style={{ width: `${transfer.progress}%` }}
                        ></div>
                      </div>
                      <span className="text-xs mt-1 block">{transfer.progress}%</span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{transfer.rows.toLocaleString()}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {new Date(transfer.startTime).toLocaleTimeString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Data Transfer Types */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-medium mb-4 flex items-center">
              <Database className="mr-2 text-blue-500" size={20} />
              ClickHouse to Flat File
            </h3>
            <div className="flex items-center justify-center p-6 bg-blue-50 rounded-lg">
              <Database className="text-blue-600 mr-4" size={48} />
              <ArrowDown className="text-blue-600 mx-4" size={36} />
              <FileText className="text-blue-600 ml-4" size={48} />
            </div>
            <div className="mt-4 text-center">
              <p className="text-gray-700">Export data from ClickHouse tables to flat files with custom delimiters</p>
              <button className="mt-4 bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded transition">
                Configure Export
              </button>
            </div>
          </div>
          
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-medium mb-4 flex items-center">
              <FileText className="mr-2 text-green-500" size={20} />
              Flat File to ClickHouse
            </h3>
            <div className="flex items-center justify-center p-6 bg-green-50 rounded-lg">
              <FileText className="text-green-600 mr-4" size={48} />
              <ArrowUp className="text-green-600 mx-4" size={36} />
              <Database className="text-green-600 ml-4" size={48} />
            </div>
            <div className="mt-4 text-center">
              <p className="text-gray-700">Import data from CSV, TSV and other flat files into ClickHouse</p>
              <button className="mt-4 bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded transition">
                Configure Import
              </button>
            </div>
          </div>
        </div>
      </main>
      
      <footer className="bg-gray-100 border-t border-gray-200 py-4 mt-8">
        <div className="container mx-auto text-center text-gray-500 text-sm">
          ClickHouse & Flat File Data Ingestion Tool © 2025
        </div>
      </footer>
    </div>
  );
}

// Helper components
function StatsCard({ title, value, icon }) {
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-500">{title}</p>
          <p className="text-2xl font-bold text-gray-900">{value}</p>
        </div>
        <div className="p-3 bg-gray-50 rounded-full">
          {icon}
        </div>
      </div>
    </div>
  );
}

function StatusBadge({ status }) {
  switch (status) {
    case 'completed':
      return <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
        <Check size={12} className="mr-1" />
        Completed
      </span>;
    case 'in-progress':
      return <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
        <RefreshCw size={12} className="mr-1 animate-spin" />
        In Progress
      </span>;
    case 'error':
      return <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
        <AlertTriangle size={12} className="mr-1" />
        Error
      </span>;
    default:
      return <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
        {status}
      </span>;
  }
}

function DirectionBadge({ direction }) {
  switch (direction) {
    case 'ch-to-ff':
      return <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
        <ArrowDown size={12} className="mr-1" />
        ClickHouse → File
      </span>;
    case 'ff-to-ch':
      return <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
        <ArrowUp size={12} className="mr-1" />
        File → ClickHouse
      </span>;
    default:
      return <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
        {direction}
      </span>;
  }
}