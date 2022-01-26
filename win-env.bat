@echo off
for /f "tokens=* eol=#" %%a in (.\env\%1) do (
    set %%a
)