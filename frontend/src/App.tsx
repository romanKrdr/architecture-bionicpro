import React from 'react';
import { ReactKeycloakProvider, useKeycloak } from '@react-keycloak/web';
import Keycloak, { KeycloakConfig } from 'keycloak-js';
import ReportPage from './components/ReportPage';

const keycloakConfig: KeycloakConfig = {
    url: process.env.REACT_APP_KEYCLOAK_URL,
    realm: process.env.REACT_APP_KEYCLOAK_REALM || '',
    clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID || '',
};

const keycloak = new Keycloak(keycloakConfig);

const AuthApp: React.FC = () => {
    const { keycloak, initialized } = useKeycloak();

    if (!initialized) {
        return (
            <div className="min-h-screen flex items-center justify-center">
                <span>Идёт инициализация…</span>
            </div>
        );
    }

    const isAuthenticated = !!keycloak.authenticated;

    const handleLogin = () => {
        keycloak.login();
    };

    const handleLogout = () => {
        keycloak.logout();
    };

    return (
        <div className="App min-h-screen bg-gray-100">
            <header className="w-full bg-white shadow mb-8">
                <div className="max-w-5xl mx-auto px-4 py-3 flex justify-between items-center">
                    <h1 className="text-lg font-semibold">BionicPRO Отчёты</h1>
                    <div>
                        {isAuthenticated ? (
                            <button
                                onClick={handleLogout}
                                className="px-3 py-1 rounded bg-gray-200 hover:bg-gray-300"
                            >
                                Выйти
                            </button>
                        ) : (
                            <button
                                onClick={handleLogin}
                                className="px-3 py-1 rounded bg-blue-600 text-white hover:bg-blue-700"
                            >
                                Войти
                            </button>
                        )}
                    </div>
                </div>
            </header>

            <main className="px-4">
                {isAuthenticated ? (
                    <ReportPage />
                ) : (
                    <div className="max-w-3xl mx-auto bg-white rounded shadow p-8">
                        <h2 className="text-xl font-semibold mb-2">Требуется авторизация</h2>
                        <p className="text-gray-700">
                            Чтобы получить отчёт по работе протеза, нажмите кнопку <b>«Войти»</b> в шапке
                            приложения и пройдите авторизацию через Keycloak.
                        </p>
                    </div>
                )}
            </main>
        </div>
    );
};

const App: React.FC = () => {
    return (
        <ReactKeycloakProvider
            authClient={keycloak}
            initOptions={{
                pkceMethod: 'S256',
            }}
        >
            <AuthApp />
        </ReactKeycloakProvider>
    );
};

export default App;