import { useState, useCallback, useRef, useEffect } from 'react';
import { api } from '../../services/api';
import { useGraphStore } from '../../store/graphStore';
import type { SearchResult } from '../../types/lineage';
import './SearchPanel.css';

const typeColors: Record<string, string> = {
  TABLE: '#22c55e',
  VIEW: '#3b82f6',
  LUA_UDF: '#f59e0b',
  VIRTUAL_SCHEMA: '#a855f7',
  CONNECTION: '#64748b',
};

export function SearchPanel() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isSearching, setIsSearching] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const timeoutRef = useRef<number | undefined>(undefined);

  const { loadLineage, isLoading } = useGraphStore();

  const handleSearch = useCallback(async (searchQuery: string) => {
    if (searchQuery.length < 1) {
      setResults([]);
      setIsOpen(false);
      return;
    }

    setIsSearching(true);
    try {
      const searchResults = await api.search(searchQuery, { limit: 15 });
      setResults(searchResults);
      setIsOpen(true);
      setSelectedIndex(-1);
    } catch (error) {
      console.error('Search failed:', error);
    } finally {
      setIsSearching(false);
    }
  }, []);

  const handleInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value;
      setQuery(value);

      // Debounce search
      if (timeoutRef.current) {
        window.clearTimeout(timeoutRef.current);
      }
      timeoutRef.current = window.setTimeout(() => {
        handleSearch(value);
      }, 200);
    },
    [handleSearch]
  );

  const handleSelectResult = useCallback(
    (result: SearchResult) => {
      setQuery(result.name);
      setIsOpen(false);
      loadLineage(result.id);
    },
    [loadLineage]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (!isOpen || results.length === 0) return;

      switch (e.key) {
        case 'ArrowDown':
          e.preventDefault();
          setSelectedIndex((prev) => (prev < results.length - 1 ? prev + 1 : prev));
          break;
        case 'ArrowUp':
          e.preventDefault();
          setSelectedIndex((prev) => (prev > 0 ? prev - 1 : -1));
          break;
        case 'Enter':
          e.preventDefault();
          if (selectedIndex >= 0 && selectedIndex < results.length) {
            handleSelectResult(results[selectedIndex]);
          }
          break;
        case 'Escape':
          setIsOpen(false);
          setSelectedIndex(-1);
          break;
      }
    },
    [isOpen, results, selectedIndex, handleSelectResult]
  );

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (inputRef.current && !inputRef.current.parentElement?.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('click', handleClickOutside);
    return () => document.removeEventListener('click', handleClickOutside);
  }, []);

  return (
    <div className="search-panel">
      <div className="search-input-wrapper">
        <svg
          className="search-icon"
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
        >
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <input
          ref={inputRef}
          type="text"
          className="search-input"
          placeholder="Search objects..."
          value={query}
          onChange={handleInputChange}
          onKeyDown={handleKeyDown}
          onFocus={() => results.length > 0 && setIsOpen(true)}
        />
        {(isSearching || isLoading) && <div className="search-spinner" />}
      </div>

      {isOpen && results.length > 0 && (
        <div className="search-results">
          {results.map((result, index) => (
            <div
              key={result.id}
              className={`search-result-item ${index === selectedIndex ? 'selected' : ''}`}
              onClick={() => handleSelectResult(result)}
              onMouseEnter={() => setSelectedIndex(index)}
            >
              <div
                className="result-type-badge"
                style={{ backgroundColor: typeColors[result.type] || '#64748b' }}
              >
                {result.type.replace('_', ' ')}
              </div>
              <div className="result-info">
                <div className="result-name">{result.name}</div>
                <div className="result-schema">{result.schema}</div>
              </div>
            </div>
          ))}
        </div>
      )}

      {isOpen && query.length > 0 && results.length === 0 && !isSearching && (
        <div className="search-results">
          <div className="no-results">No objects found</div>
        </div>
      )}
    </div>
  );
}
