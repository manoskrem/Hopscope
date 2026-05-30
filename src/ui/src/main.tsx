import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

// Self-hosted fonts (bundled by Fontsource — no runtime CDN).
import '@fontsource-variable/jetbrains-mono/index.css';
import '@fontsource/chakra-petch/500.css';
import '@fontsource/chakra-petch/700.css';

import './styles/theme.css';
import { App } from './App';

const root = document.getElementById('root');
if (!root) throw new Error('#root element not found');

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
