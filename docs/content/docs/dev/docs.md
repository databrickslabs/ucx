---
linkTitle: "Writing documentation"
title: "Writing documentation"
---

UCX documentation uses the following set of technologies:

- [Hugo](https://gohugo.io/) - Static site generator written in Go
- [Hextra](https://imfing.github.io/hextra/) - Hugo theme for documentation
- [TailwindCSS](https://tailwindcss.com/) - Utility-first CSS framework, mainly used for custom styling


## Prerequisites

To build the documentation, you need to have the following tools installed:
- [Hugo](https://gohugo.io/getting-started/installing/)
- [Node.js](https://nodejs.org/en/download/)
- [Yarn](https://classic.yarnpkg.com/en/docs/install/)

{{<callout type="info">}}
The documentation is built using Hugo, a static site generator. The content is written in Markdown and the site is styled using TailwindCSS. We only need node.js and yarn to make tailwindcss work.
{{</callout>}}

## Installing tailwindcss

To install the dependencies, run the following commands:

```bash
cd docs && yarn install
```

## Installing Hugo

To install Hugo, follow the instructions on the [Hugo website](https://gohugo.io/getting-started/installing/).

## Running the documentation locally

To run the documentation locally, run the following command:

```bash
cd docs && hugo server
```

Follow the instructions in the terminal to open the documentation in your browser. The default URL is `http://localhost:1313/ucx`.

## Adding static files or images

To add static files or images, place them in the `docs/static` directory. It's recommended to put images in the `docs/static/images` directory.

You can reference them in your markdown files using the following syntax:

```markdown
![Alt text](/images/image.png)
```

## Using shortcodes

Shortcodes are used to add reusable components to the documentation. You can find the list of available shortcodes in the [Hextra documentation](https://imfing.github.io/hextra/).