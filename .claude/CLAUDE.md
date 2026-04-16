# The project
KPI debugger, a reasoning engine on top of your metrics. It doesn't just show you analytics or 
identifies anomalies, it actively finds issues, correlates them across different data sources, finds root
causes, and tells you what to fix.

## Common infrastructure
- Users can login using username/password or Google (using Firebase)
- Projects are the main entity that users work on. Users can be assigned to projects with different access rights:
  - Read: Can see all metrics, analytics, advice
  - Edit: Can create new metrics etc.
  - Admin: Can delete projects, connect data sources, ...


# Architecture

## Structure
The following folders are relevant
- `kpidebug/common`: Common utilities and types
- `kpidebug/data`: Raw data sources and integrations
  - `kpidebug/data/stripe`: Stripe data source
- `kpidebug/metrics`: Metrics engine
- `ui`: Frontend
  - `ui/src/api`: API client and types
  - `ui/src/components`: Components
  - `ui/src/layout`: Overall app layout
  - `ui/src/pages`: Pages
- `tests`: Tests. The structure under tests mirrors the structure under `kpidebug`
- `scripts`: Helper and setup scripts

## Backend
The backend is written in Python 3.13. 
- Use the virtual environment in `.venv`, pip to install dependencies and keep `requirements.txt` current
- Use `.env` to store environment variables

## Frontend
The frontend is written in Typescript with React.
- We use create react app for the main setup with Typescript
- We use MUI for the frontend components
- We use Firebase for authentication
- The UI comminucates via REST APIs with the backend, the client is generated from the OpenAPI endpoint

# Patterns


# Style

## Backend Code
- In general, stick to the conventions you already find in existing code
- Always use types. Don't use untyped arguments and avoid Any if possible. Don't return dicts but proper types.
- Declare properties of types explicitely, don't just assign them in constructors.
- Avoid circular imports. Alert if one is about to be introduct, don't just blindly solve with TYPE_CHECKING
- Use dataclasses and dataclasses-json for schema and data model classes
- Use `self` not `cls` for class methods. Don't use the @classmethod annotations
- Prefer `__init__` to `new`

## Backend Testing
- Write tests for all major functionality. Test a common path and a few edge cases.
- Follow the code structure when writing tests, i.e. one test module / file for each code test / file. One test class for each code class. 
- Run and make sure the tests pass

## Frontend code

## Frontend testing
- No need for frontend tests