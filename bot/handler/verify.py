from __future__ import annotations
from typing import Any, Dict, List, Optional
import discord

# 인증 기능을 우회하고 에러를 방지하는 최소화 버전
class StoveConnectView(discord.ui.View):
    def __init__(self, **kwargs):
        super().__init__(timeout=None)

class StoveLinkedDropdownView(discord.ui.View):
    def __init__(self, *args, **kwargs):
        super().__init__(timeout=None)

class StoveSettingsView(discord.ui.View):
    def __init__(self, *args, **kwargs):
        super().__init__(timeout=None)

class CharacterVerifyModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="캐릭터 등록")
        self.add_item(
            discord.ui.InputText(
                label="캐릭터 이름",
                placeholder="이름을 입력하면 즉시 등록됩니다.",
                required=True
            )
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_message(f"**{self.children[0].value}** 님, 등록이 완료되었습니다! (인증 생략)", ephemeral=True)

async def handle_verify_button(interaction: discord.Interaction):
    # 버튼 클릭 시 위에서 만든 입력창을 띄워줌
    await interaction.response.send_modal(CharacterVerifyModal())

# 나머지 에러 방지용 가짜 함수들
async def _run_character_verify(*args, **kwargs): pass
async def _run_character_verify_from_stove_selection(*args, **kwargs): pass
