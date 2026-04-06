from __future__ import annotations
from typing import Any, Dict, List, Optional
import discord

# 인증 기능을 완전히 제거하고 '즉시 성공'시키기 위한 단순화된 코드입니다.

class CharacterVerifyModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="로스트아크 간편 등록")
        self.add_item(
            discord.ui.InputText(
                label="캐릭터 이름",
                placeholder="등록할 캐릭터 이름을 입력하세요",
                required=True,
                max_length=16,
            )
        )

    async def callback(self, interaction: discord.Interaction):
        # 1. 일단 응답 대기
        await interaction.response.defer(ephemeral=True)
        character_name = (self.children[0].value or "").strip()
        
        # 2. API 체크 없이 바로 환영 메시지 출력
        embed = discord.Embed(
            title="등록 완료",
            description=f"**{character_name}** 님이 성공적으로 등록되었습니다!",
            color=discord.Color.green(),
        )
        embed.add_field(name="안내", value="인증 기능이 비활성화되어 바로 등록되었습니다.", inline=False)
        
        await interaction.followup.send(embed=embed, ephemeral=True)

# 기존에 다른 파일에서 이 클래스들을 참조할 수 있으므로, 
# 에러 방지를 위해 빈 껍데기 클래스들을 남겨둡니다.

class StoveConnectView(discord.ui.View):
    def __init__(self, **kwargs):
        super().__init__(timeout=None)
        # 버튼을 눌러도 아무 동작 안 하거나 안내 메시지만 출력
        pass

class StoveLinkedDropdownView(discord.ui.View): # DesignerView -> View로 변경
    def __init__(self, *args, **kwargs):
        super().__init__(timeout=None)

class StoveSettingsView(discord.ui.View): # DesignerView -> View로 변경
    def __init__(self, *args, **kwargs):
        super().__init__(timeout=None)

# 봇 실행 중 에러를 막기 위한 가짜 함수들
async def _run_character_verify(interaction, character_name):
    pass

async def handle_verify_button(interaction: discord.Interaction):
    # 버튼 클릭 시 모달창(이름 입력창)만 띄워줌
    await interaction.response.send_modal(CharacterVerifyModal())
