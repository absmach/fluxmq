import { mount } from 'svelte';
import App from './App.svelte';
import './style.css';
import './lab.css';

mount(App, { target: document.getElementById('app')! });
