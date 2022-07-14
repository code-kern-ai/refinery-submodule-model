# refinery-submodule-s3
[![refinery repository](https://uploads-ssl.webflow.com/61e47fafb12bd56b40022a49/62c2f30f935f4d37dc864eeb_Kern%20refinery.png)](https://github.com/code-kern-ai/refinery)

Data model for [refinery](https://github.com/code-kern-ai/refinery). Manages entities and their access for multiple services, e.g. the [gateway](https://github.com/code-kern-ai/refinery-gateway).

*Caution! This is only meant as a submodule not as a library*

If you like what we're working on, please leave a ⭐ for [refinery](https://github.com/code-kern-ai/refinery)!

## Usage

Implement by using:
`git submodule add git@github.com:code-kern-ai/submodule-model.git submodules/model`

This means the requirements of the project need to include:

```
sqlalchemy==1.4.25
graphene_sqlalchemy==2.3.0
alembic==1.7.1
psycopg2-binary==2.9.1
```
(version numbers may change)

Also one os variable is required to access database:
`- POSTGRES`


## Common submodule logic applies
[git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules)

Submodules can be a bit irritating - key insights:

1. Paths need to be relative inside the module (e.g. `from . import constants` )
2. They are their own repositories so they need to be handled as such (committing etc. for changes inside their own branch)
3. Cloning needs to be done recursively to get the files
4. Adding submodules without changing the folder will result in errors because the import of paths with `-` doesn't work
5. Drone builds appear to work fine
6. Drone builds need to include the "collection" of the submodules data so another step needs to be introduces to the .yml file:

```
- name: submodules
  image: alpine/git
  commands:
    - git submodule init
    - 'git config --global url."https://github.com/".insteadOf git@github.com:'
    - "git submodule update --recursive --remote"
```

**"remember" to pull / push for the submodules!!**

## Example Commands for submodules

### Clone reposity with submodules

`git clone <git@github ...> --recursive`
Example
`git clone git@github.com:code-kern-ai/refinery-gateway.git --recursive`

(if you missed the --recursive part or switch to a branch with a new submodule):
`git submodule update --init`

### Add submodule

(no path specified -> the repository will be a new folder on the top level - **DON'T DO THIS**)

`git submodule add <git@github ...> snipmate-snippets/snippets/`

(path specified -> this one we usually want since repository names might have some issues)

`git submodule add <git@github ...> submodules/<repo_name_with_underscores>`

### Working with submodules after initial setup

- Either navigate to the submodule folder and use it like every other git:
  - `cd submodules/<repo_name_with_underscores>`
  - `git pull`
  - `git add .`
  - `git commit ....`
  - `git push`
- Or use submodule command like:
  - `git submodule foreach git pull`
  - `git submodule foreach git add .`
  - `git submodule foreach git commit -m '<message>'`
  - `git submodule foreach git push`
