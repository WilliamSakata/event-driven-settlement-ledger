module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2022,
    sourceType: 'module',
  },
  plugins: ['@typescript-eslint', 'boundaries'],
  extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended'],
  env: {
    node: true,
    es2022: true,
  },
  settings: {
    'import/resolver': {
      node: {
        extensions: ['.ts', '.js'],
      },
    },
    'boundaries/elements': [
      { type: 'domain', pattern: 'src/domain/**' },
      { type: 'application', pattern: 'src/application/**' },
      { type: 'adapters', pattern: 'src/adapters/**' },
      { type: 'config', pattern: 'src/config/**' },
      { type: 'main', pattern: 'src/main.ts' },
    ],
  },
  rules: {
    '@typescript-eslint/no-unused-vars': [
      'error',
      { argsIgnorePattern: '^_' },
    ],
    'boundaries/dependencies': [
      2,
      {
        default: 'disallow',
        policies: [
          {
            from: { element: { type: 'domain' } },
            allow: { to: { element: { type: 'domain' } } },
          },
          {
            from: { element: { type: 'application' } },
            allow: { to: { element: { types: { anyOf: ['domain', 'application'] } } } },
          },
          {
            from: { element: { type: 'adapters' } },
            allow: {
              to: { element: { types: { anyOf: ['domain', 'application', 'adapters', 'config'] } } },
            },
          },
          {
            from: { element: { type: 'config' } },
            allow: { to: { element: { type: 'config' } } },
          },
          {
            from: { element: { type: 'main' } },
            allow: {
              to: { element: { types: { anyOf: ['domain', 'application', 'adapters', 'config'] } } },
            },
          },
        ],
      },
    ],
  },
};
