EXPORT_HTML_TEMPLATE = """
<style>
    @font-face {{
        font-family: 'DM Sans';
        src: url(https://cdn.bfldr.com/9AYANS2F/at/p9qfs3vgsvnp5c7txz583vgs/dm-sans-regular.ttf?auto=webp&format=ttf) format('truetype');
    }}
    body {{ font-family: 'DM Sans', Arial, sans-serif; }}
    .export-container {{ text-align: center; margin-top: 20px; }}
    .export-container h2 {{ color: #1B3139; font-size: 24px; margin-bottom: 20px; }}
    .export-container button {{
        display: inline-block; padding: 12px 25px; background-color: #1B3139;
        color: #fff; border: none; border-radius: 4px; font-size: 18px;
        font-weight: 500; cursor: pointer; transition: background-color 0.3s, transform 0.3s;
    }}
    .export-container button:hover {{ background-color: #FF3621; transform: translateY(-2px); }}
</style>

<div class="export-container">
    <h2>Export Results</h2>
    <button onclick="downloadExcel()">Download Results</button>
</div>

<script>
    function downloadExcel() {{
        const b64Data = '{b64_data}';
        const filename = '{export_file_path_name}';

        // Convert base64 to blob
        const byteCharacters = atob(b64Data);
        const byteNumbers = new Array(byteCharacters.length);
        for (let i = 0; i < byteCharacters.length; i++) {{
            byteNumbers[i] = byteCharacters.charCodeAt(i);
        }}
        const byteArray = new Uint8Array(byteNumbers);
        const blob = new Blob([byteArray], {{
            type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }});

        // Create download link and click it
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
    }}
</script>
"""
